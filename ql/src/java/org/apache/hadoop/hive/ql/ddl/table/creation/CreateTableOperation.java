/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.ddl.table.creation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * Operation process of creating a table.
 */
public class CreateTableOperation extends DDLOperation<CreateTableDesc> {
  public CreateTableOperation(DDLOperationContext context, CreateTableDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // create the table
    Table tbl = desc.toTable(context.getConf());
    LOG.debug("creating table {} on {}", tbl.getFullyQualifiedName(), tbl.getDataLocation());

    if (desc.getReplicationSpec().isInReplicationScope() && (!desc.getReplaceMode())){
      // if this is a replication spec, then replace-mode semantics might apply.
      // if we're already asking for a table replacement, then we can skip this check.
      // however, otherwise, if in replication scope, and we've not been explicitly asked
      // to replace, we should check if the object we're looking at exists, and if so,
      // trigger replace-mode semantics.
      Table existingTable = context.getDb().getTable(tbl.getDbName(), tbl.getTableName(), false);
      if (existingTable != null){
        if (desc.getReplicationSpec().allowEventReplacementInto(existingTable.getParameters())) {
          desc.setReplaceMode(true); // we replace existing table.
          ReplicationSpec.copyLastReplId(existingTable.getParameters(), tbl.getParameters());
        } else {
          LOG.debug("DDLTask: Create Table is skipped as table {} is newer than update", desc.getTableName());
          return 0; // no replacement, the existing table state is newer than our update.
        }
      }
    }

    // create the table
    if (desc.getReplaceMode()) {
      createTableReplaceMode(tbl);
    } else {
      createTableNonReplaceMode(tbl);
    }

    DDLUtils.addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK), context);
    return 0;
  }

  private void createTableReplaceMode(Table tbl) throws HiveException {
    ReplicationSpec replicationSpec = desc.getReplicationSpec();
    long writeId = 0;
    EnvironmentContext environmentContext = null;
    if (replicationSpec != null && replicationSpec.isInReplicationScope()) {
      if (replicationSpec.isMigratingToTxnTable()) {
        // for migration we start the transaction and allocate write id in repl txn task for migration.
        String writeIdPara = context.getConf().get(ReplUtils.REPL_CURRENT_TBL_WRITE_ID);
        if (writeIdPara == null) {
          throw new HiveException("DDLTask : Write id is not set in the config by open txn task for migration");
        }
        writeId = Long.parseLong(writeIdPara);
      } else {
        writeId = desc.getReplWriteId();
      }

      // In case of replication statistics is obtained from the source, so do not update those
      // on replica. Since we are not replicating statisics for transactional tables, do not do
      // so for transactional tables right now.
      if (!AcidUtils.isTransactionalTable(desc)) {
        environmentContext = new EnvironmentContext();
        environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
      }
    }

    // replace-mode creates are really alters using CreateTableDesc.
    context.getDb().alterTable(tbl.getCatName(), tbl.getDbName(), tbl.getTableName(), tbl, false, environmentContext,
        true, writeId);
  }

  private void createTableNonReplaceMode(Table tbl) throws HiveException {
    if (CollectionUtils.isNotEmpty(desc.getPrimaryKeys()) ||
        CollectionUtils.isNotEmpty(desc.getForeignKeys()) ||
        CollectionUtils.isNotEmpty(desc.getUniqueConstraints()) ||
        CollectionUtils.isNotEmpty(desc.getNotNullConstraints()) ||
        CollectionUtils.isNotEmpty(desc.getDefaultConstraints()) ||
        CollectionUtils.isNotEmpty(desc.getCheckConstraints())) {
      context.getDb().createTable(tbl, desc.getIfNotExists(), desc.getPrimaryKeys(), desc.getForeignKeys(),
          desc.getUniqueConstraints(), desc.getNotNullConstraints(), desc.getDefaultConstraints(),
          desc.getCheckConstraints());
    } else {
      context.getDb().createTable(tbl, desc.getIfNotExists());
    }

    if (desc.isCTAS()) {
      Table createdTable = context.getDb().getTable(tbl.getDbName(), tbl.getTableName());
      DataContainer dc = new DataContainer(createdTable.getTTable());
      context.getQueryState().getLineageState().setLineage(createdTable.getPath(), dc, createdTable.getCols());
    }
  }

  public static boolean doesTableNeedLocation(Table tbl) {
    // TODO: If we are ok with breaking compatibility of existing 3rd party StorageHandlers,
    // this method could be moved to the HiveStorageHandler interface.
    boolean retval = true;
    if (tbl.getStorageHandler() != null) {
      // TODO: why doesn't this check class name rather than toString?
      String sh = tbl.getStorageHandler().toString();
      retval = !"org.apache.hadoop.hive.hbase.HBaseStorageHandler".equals(sh) &&
          !Constants.DRUID_HIVE_STORAGE_HANDLER_ID.equals(sh) &&
          !Constants.JDBC_HIVE_STORAGE_HANDLER_ID.equals(sh) &&
          !"org.apache.hadoop.hive.accumulo.AccumuloStorageHandler".equals(sh);
    }
    return retval;
  }

  public static void makeLocationQualified(Table table, HiveConf conf) throws HiveException {
    StorageDescriptor sd = table.getTTable().getSd();
    // If the table's location is currently unset, it is left unset, allowing the metastore to
    // fill in the table's location.
    // Note that the previous logic for some reason would make a special case if the DB was the
    // default database, and actually attempt to generate a  location.
    // This seems incorrect and uncessary, since the metastore is just as able to fill in the
    // default table location in the case of the default DB, as it is for non-default DBs.
    Path path = null;
    if (sd.isSetLocation()) {
      path = new Path(sd.getLocation());
    }
    if (path != null) {
      sd.setLocation(Utilities.getQualifiedPath(conf, path));
    }
  }
}
