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
package org.apache.hadoop.hive.metastore.cache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.HiveAlterHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.cache.SharedCache.StatsType;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregatorFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.UpdateTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.messaging.DeleteTableColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.UpdatePartitionColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.DeletePartitionColumnStatMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

// TODO filter->expr
// TODO functionCache
// TODO constraintCache
// TODO need sd nested copy?
// TODO String intern
// TODO monitor event queue
// TODO initial load slow?
// TODO size estimation

public class CachedStore implements RawStore, Configurable {
  private static ScheduledExecutorService cacheUpdateMaster = null;
  private static List<Pattern> whitelistPatterns = null;
  private static List<Pattern> blacklistPatterns = null;
  // Default value set to 100 milliseconds for test purpose
  private static long DEFAULT_CACHE_REFRESH_PERIOD = 100;
  // Time after which metastore cache is updated from metastore DB by the background update thread
  private static long cacheRefreshPeriodMS = DEFAULT_CACHE_REFRESH_PERIOD;
  private static int MAX_RETRIES = 10;
  // This is set to true only after prewarm is complete
  private static AtomicBoolean isCachePrewarmed = new AtomicBoolean(false);
  // This is set to true only if we were able to cache all the metadata.
  // We may not be able to cache all metadata if we hit CACHED_RAW_STORE_MAX_CACHE_MEMORY limit.
  private static AtomicBoolean isCachedAllMetadata = new AtomicBoolean(false);
  private static TablesPendingPrewarm tblsPendingPrewarm = new TablesPendingPrewarm();
  private RawStore rawStore = null;
  private Configuration conf;
  private static boolean areTxnStatsSupported;
  private PartitionExpressionProxy expressionProxy = null;
  private static String startUpdateServiceLock = "L";
  private static String lock = "L";
  private static boolean sharedCacheInited = false;
  private static SharedCache sharedCache = new SharedCache();
  private static long lastEventId;
  private static Map<Long, Map<String, Long>> txnIdToWriteId = new HashMap<>();
  private static boolean counterInited = false;
  private static Counter cacheHit;
  private static Counter cacheMiss;

  private static final Logger LOG = LoggerFactory.getLogger(CachedStore.class.getName());

  @Override public void setConf(Configuration conf) {
    if (MetastoreConf.getVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS)==null ||
        MetastoreConf.getVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS).isEmpty()) {
      throw new RuntimeException("CahcedStore can not use events for invalidation as there is no " +
          " TransactionalMetaStoreEventListener to add events to notification table");
    }
    if (!counterInited && MetastoreConf.getBoolVar(conf, ConfVars.METRICS_ENABLED)) {
      Metrics.initialize(conf);
      cacheHit = Metrics.getOrCreateCounter(MetricsConstants.METADATA_CACHE_HIT);
      cacheMiss = Metrics.getOrCreateCounter(MetricsConstants.METADATA_CACHE_MISS);
      counterInited = true;
    }
    setConfInternal(conf);
    initBlackListWhiteList(conf);
    initSharedCache(conf);
    startCacheUpdateService(conf, false, true);
  }

  /**
   * Similar to setConf but used from within the tests
   * This does start the background thread for prewarm and update
   * @param conf
   */
  void setConfForTest(Configuration conf) {
    setConfForTestExceptSharedCache(conf);
    initSharedCache(conf);
  }

  void setConfForTestExceptSharedCache(Configuration conf) {
    setConfInternal(conf);
    initBlackListWhiteList(conf);
  }

  private static synchronized void triggerUpdateUsingEvent(RawStore rawStore, Configuration conf) {
    if (!isCachePrewarmed.get()) {
      LOG.error("cache update should be done only after prewarm");
      throw new RuntimeException("cache update should be done only after prewarm");
    }
    long startTime = System.nanoTime();
    long preEventId = lastEventId;
    try {
      lastEventId = updateUsingNotificationEvents(rawStore, lastEventId, conf);
    } catch (Exception e) {
      LOG.error(" cache update failed for start event id " + lastEventId + " with error ", e);
      throw new RuntimeException(e.getMessage());
    } finally {
      long endTime = System.nanoTime();
      LOG.info("Time taken in updateUsingNotificationEvents for num events : " + (lastEventId - preEventId) + " = "
          + (endTime - startTime) / 1000000 + "ms");
    }
  }

  private static synchronized void triggerPreWarm(RawStore rawStore, Configuration conf) {
    lastEventId = rawStore.getCurrentNotificationEventId().getEventId();
    prewarm(rawStore, conf);
  }

  private void setConfInternal(Configuration conf) {
    String rawStoreClassName = MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL, ObjectStore.class.getName());
    if (rawStore == null) {
      try {
        rawStore = (JavaUtils.getClass(rawStoreClassName, RawStore.class)).newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
      }
    }
    rawStore.setConf(conf);
    Configuration oldConf = this.conf;
    this.conf = conf;
    this.areTxnStatsSupported = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_TXN_STATS_ENABLED);
    if (expressionProxy != null && conf != oldConf) {
      LOG.warn("Unexpected setConf when we were already configured");
    } else {
      expressionProxy = PartFilterExprUtil.createExpressionProxy(conf);
    }
  }

  private void initSharedCache(Configuration conf) {
    synchronized (lock) {
      if (!sharedCacheInited) {
        sharedCacheInited = true;
        sharedCache.initialize(conf);
      }
    }
  }

  @VisibleForTesting public static SharedCache getSharedCache() {
    return sharedCache;
  }

  private static ColumnStatistics updateStatsForAlterPart(RawStore rawStore, Table before, String catalogName,
      String dbName, String tableName, Partition part) throws Exception {
    ColumnStatistics colStats;
    List<String> deletedCols = new ArrayList<>();
    colStats = HiveAlterHandler
        .updateOrGetPartitionColumnStats(rawStore, catalogName, dbName, tableName, part.getValues(),
            part.getSd().getCols(), before, part, null, deletedCols);
    for (String column : deletedCols) {
      sharedCache.removePartitionColStatsFromCache(catalogName, dbName, tableName, part.getValues(), column);
    }
    if (colStats != null) {
      sharedCache.alterPartitionAndStatsInCache(catalogName, dbName, tableName, part.getWriteId(), part.getValues(),
          part.getParameters(), colStats.getStatsObj());
    }
    return colStats;
  }

  private static void updateStatsForAlterTable(RawStore rawStore, Table tblBefore, Table tblAfter, String catalogName,
      String dbName, String tableName) throws Exception {
    ColumnStatistics colStats = null;
    List<String> deletedCols = new ArrayList<>();
    if (tblBefore.isSetPartitionKeys()) {
      List<Partition> parts = sharedCache.listCachedPartitions(catalogName, dbName, tableName, -1);
      for (Partition part : parts) {
        colStats = updateStatsForAlterPart(rawStore, tblBefore, catalogName, dbName, tableName, part);
      }
    }

    List<ColumnStatisticsObj> statisticsObjs = HiveAlterHandler
        .alterTableUpdateTableColumnStats(rawStore, tblBefore, tblAfter, null, null, rawStore.getConf(), deletedCols);
    if (colStats != null) {
      sharedCache.alterTableAndStatsInCache(catalogName, dbName, tableName, tblAfter.getWriteId(), statisticsObjs,
          tblAfter.getParameters());
    }
    for (String column : deletedCols) {
      sharedCache.removeTableColStatsFromCache(catalogName, dbName, tableName, column);
    }
  }

  @VisibleForTesting public static long updateUsingNotificationEvents(RawStore rawStore, long lastEventId, Configuration conf)
      throws Exception {
    Deadline.registerIfNot(1000000);
    LOG.debug("updating cache using notification events starting from event id " + lastEventId);
    NotificationEventRequest rqst = new NotificationEventRequest(lastEventId);

    //Add the events which are not related to metadata update
    rqst.addToEventTypeSkipList(MessageBuilder.INSERT_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ACID_WRITE_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.CREATE_FUNCTION_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.DROP_FUNCTION_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ADD_PRIMARYKEY_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ADD_FOREIGNKEY_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ADD_UNIQUECONSTRAINT_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ADD_NOTNULLCONSTRAINT_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.DROP_CONSTRAINT_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.CREATE_ISCHEMA_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ALTER_ISCHEMA_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.DROP_ISCHEMA_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ADD_SCHEMA_VERSION_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.ALTER_SCHEMA_VERSION_EVENT);
    rqst.addToEventTypeSkipList(MessageBuilder.DROP_SCHEMA_VERSION_EVENT);

    Deadline.startTimer("getNextNotification");
    NotificationEventResponse resp = rawStore.getNextNotification(rqst);
    Deadline.stopTimer();

    if (resp == null || resp.getEvents() == null) {
      LOG.debug("no events to process");
      return lastEventId;
    }

    List<NotificationEvent> eventList = resp.getEvents();
    LOG.debug("num events to process" + eventList.size());

    for (NotificationEvent event : eventList) {
      long eventId = event.getEventId();
      if (eventId <= lastEventId) {
        LOG.error("Event id is not valid " + lastEventId + " : " + eventId);
        throw new RuntimeException(" event id is not valid " + lastEventId + " : " + eventId);
      }
      lastEventId = eventId;
      String message = event.getMessage();
      LOG.debug("Event to process " + event);
      MessageDeserializer deserializer = MessageFactory.getInstance(event.getMessageFormat()).getDeserializer();
      String catalogName = event.getCatName() == null ? "" : event.getCatName().toLowerCase();
      String dbName = event.getDbName() == null ? "" : event.getDbName().toLowerCase();
      String tableName = event.getTableName() == null ? "" : event.getTableName().toLowerCase();
      if (!shouldCacheTable(catalogName, dbName, tableName)) {
        continue;
      }
      switch (event.getEventType()) {
      case MessageBuilder.ADD_PARTITION_EVENT:
        AddPartitionMessage addPartMessage = deserializer.getAddPartitionMessage(message);
        sharedCache.addPartitionsToCache(catalogName, dbName, tableName, addPartMessage.getPartitionObjs());
        break;
      case MessageBuilder.ALTER_PARTITION_EVENT:
        AlterPartitionMessage alterPartitionMessage = deserializer.getAlterPartitionMessage(message);
        sharedCache
            .alterPartitionInCache(catalogName, dbName, tableName, alterPartitionMessage.getPtnObjBefore().getValues(),
                alterPartitionMessage.getPtnObjAfter());
        //TODO : Use the stat object stored in the alter table message to update the stats in cache.
        updateStatsForAlterPart(rawStore, alterPartitionMessage.getTableObj(), catalogName, dbName, tableName,
            alterPartitionMessage.getPtnObjAfter());
        break;
      case MessageBuilder.DROP_PARTITION_EVENT:
        DropPartitionMessage dropPartitionMessage = deserializer.getDropPartitionMessage(message);
        for (Map<String, String> partMap : dropPartitionMessage.getPartitions()) {
          sharedCache.removePartitionFromCache(catalogName, dbName, tableName, new ArrayList<>(partMap.values()));
        }
        break;
      case MessageBuilder.CREATE_TABLE_EVENT:
        CreateTableMessage createTableMessage = deserializer.getCreateTableMessage(message);
        sharedCache.addTableToCache(catalogName, dbName, tableName, createTableMessage.getTableObj(), newTableWriteIds(dbName, tableName));
        break;
      case MessageBuilder.ALTER_TABLE_EVENT:
        AlterTableMessage alterTableMessage = deserializer.getAlterTableMessage(message);
        sharedCache.alterTableInCache(catalogName, dbName, tableName, alterTableMessage.getTableObjAfter());
        //TODO : Use the stat object stored in the alter table message to update the stats in cache.
        updateStatsForAlterTable(rawStore, alterTableMessage.getTableObjBefore(), alterTableMessage.getTableObjAfter(),
            catalogName, dbName, tableName);
        break;
      case MessageBuilder.DROP_TABLE_EVENT:
        DropTableMessage dropTableMessage = deserializer.getDropTableMessage(message);
        int batchSize = MetastoreConf.getIntVar(rawStore.getConf(), ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);
        String tableDnsPath = null;
        Path tablePath = dropTableMessage.getTableObj().getSd().getLocation()!=null?
            new Path(dropTableMessage.getTableObj().getSd().getLocation()):null;
        if (tablePath != null) {
          tableDnsPath = new Warehouse(rawStore.getConf()).getDnsPath(tablePath).toString();
        }

        while (true) {
          Map<String, String> partitionLocations = rawStore.getPartitionLocations(catalogName, dbName, tableName,
              tableDnsPath, batchSize, null);
          if (partitionLocations == null || partitionLocations.isEmpty()) {
            break;
          }
          sharedCache
              .removePartitionFromCache(catalogName, dbName, tableName, new ArrayList<>(partitionLocations.values()));
        }
        sharedCache.removeTableFromCache(catalogName, dbName, tableName);
        break;
      case MessageBuilder.UPDATE_TBL_COL_STAT_EVENT:
        UpdateTableColumnStatMessage msg = deserializer.getUpdateTableColumnStatMessage(message);
        Table tbl = msg.getTableObject();
        Map<String, String> newParams = new HashMap<>(tbl.getParameters());
        List<String> colNames = new ArrayList<>();
        for (ColumnStatisticsObj statsObj : msg.getColumnStatistics().getStatsObj()) {
          colNames.add(statsObj.getColName());
        }
        StatsSetupConst.setColumnStatsState(newParams, colNames);
        long writeId = msg.getWriteId();
        String validWriteIds = msg.getWriteIds();
        if (validWriteIds != null) {
          if (!areTxnStatsSupported) {
            StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
          } else {
            String errorMsg = ObjectStore.verifyStatsChangeCtx(TableName.getDbTable(dbName, tableName),
                tbl.getParameters(), newParams, writeId, validWriteIds, true);
            if (errorMsg != null) {
              throw new MetaException(errorMsg);
            }
            if (!ObjectStore.isCurrentStatsValidForTheQuery(newParams, writeId, validWriteIds, true)) {
              // Make sure we set the flag to invalid regardless of the current value.
              StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
              LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the table "
                  + dbName + "." + tableName);
            }
          }
        }
        sharedCache.alterTableAndStatsInCache(catalogName, dbName, tableName, msg.getWriteId(),
            msg.getColumnStatistics().getStatsObj(), msg.getParameters());
        break;
      case MessageBuilder.DELETE_TBL_COL_STAT_EVENT:
        DeleteTableColumnStatMessage msgDel = deserializer.getDeleteTableColumnStatMessage(message);
        sharedCache.removeTableColStatsFromCache(catalogName, dbName, tableName, msgDel.getColName());
        break;
      case MessageBuilder.UPDATE_PART_COL_STAT_EVENT:
        UpdatePartitionColumnStatMessage msgPartUpdate = deserializer.getUpdatePartitionColumnStatMessage(message);
        Partition partition = sharedCache.getPartitionFromCache(catalogName, dbName, tableName, msgPartUpdate.getPartVals());
        newParams = new HashMap<>(partition.getParameters());
        colNames = new ArrayList<>();
        for (ColumnStatisticsObj statsObj : msgPartUpdate.getColumnStatistics().getStatsObj()) {
          colNames.add(statsObj.getColName());
        }
        StatsSetupConst.setColumnStatsState(newParams, colNames);
        writeId = msgPartUpdate.getWriteId();
        validWriteIds = msgPartUpdate.getWriteIds();
        if (validWriteIds != null) {
          if (!areTxnStatsSupported) {
            StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
          } else {
            String errorMsg = ObjectStore.verifyStatsChangeCtx(TableName.getDbTable(dbName, tableName),
                partition.getParameters(), newParams, writeId, validWriteIds, true);
            if (errorMsg != null) {
              throw new MetaException(errorMsg);
            }
            if (!ObjectStore.isCurrentStatsValidForTheQuery(newParams, writeId, validWriteIds, true)) {
              // Make sure we set the flag to invalid regardless of the current value.
              StatsSetupConst.setBasicStatsState(newParams, StatsSetupConst.FALSE);
              LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the partition "
                  + dbName + "." + tableName + "." + msgPartUpdate.getPartVals());
            }
          }
        }
        sharedCache.alterPartitionAndStatsInCache(catalogName, dbName, tableName, msgPartUpdate.getWriteId(),
            msgPartUpdate.getPartVals(), msgPartUpdate.getParameters(),
            msgPartUpdate.getColumnStatistics().getStatsObj());
        break;
      case MessageBuilder.DELETE_PART_COL_STAT_EVENT:
        DeletePartitionColumnStatMessage msgPart = deserializer.getDeletePartitionColumnStatMessage(message);
        sharedCache.removePartitionColStatsFromCache(catalogName, dbName, tableName, msgPart.getPartValues(),
            msgPart.getColName());
        break;
      case MessageBuilder.OPEN_TXN_EVENT:
        OpenTxnMessage openTxn = deserializer.getOpenTxnMessage(message);
        for (long txnId : openTxn.getTxnIds()) {
          txnIdToWriteId.put(txnId, new HashMap<>());
        }
        break;
      case MessageBuilder.ABORT_TXN_EVENT:
        AbortTxnMessage abortTxn = deserializer.getAbortTxnMessage(message);
        txnIdToWriteId.put(abortTxn.getTxnId(), new HashMap<>());
        break;
      case MessageBuilder.ALLOC_WRITE_ID_EVENT:
        AllocWriteIdMessage allocWriteId = deserializer.getAllocWriteIdMessage(message);
        List<TxnToWriteId> txnToWriteIdList = allocWriteId.getTxnToWriteIdList();
        for (TxnToWriteId txnToWriteId : txnToWriteIdList) {
          long txnId = txnToWriteId.getTxnId();
          if (txnIdToWriteId.containsKey(txnId)) {
            Map<String, Long> m = txnIdToWriteId.get(txnId);
            String fullTableName = TableName.getDbTable(dbName, tableName);
            m.put(fullTableName, txnToWriteId.getWriteId());
          }
        }
        break;
        case MessageBuilder.COMMIT_TXN_EVENT:
          CommitTxnMessage commitTxn = deserializer.getCommitTxnMessage(message);
          if (txnIdToWriteId.containsKey(commitTxn.getTxnId())) {
            Map<String, Long> m = txnIdToWriteId.get(commitTxn.getTxnId());
            for (Map.Entry<String, Long> entry : m.entrySet()) {
              String tblNameToFlag = entry.getKey();
              long writeIdToCommit = entry.getValue();
              TableName tname = TableName.fromString(tblNameToFlag, Warehouse.DEFAULT_CATALOG_NAME, Warehouse.DEFAULT_DATABASE_NAME);
              sharedCache.commitWriteId(tname.getCat(), tname.getDb(), tname.getTable(), writeIdToCommit);
            }
            txnIdToWriteId.remove(commitTxn.getTxnId());
          } else {
            GetTxnTableWriteIdsResponse getTxnTableWriteIdsResponse = HMSHandler.getMsThreadTxnHandler(conf)
                .getTxnTableWriteIds(commitTxn.getTxnId());
            for (TableWriteId tableWriteId : getTxnTableWriteIdsResponse.getTableWriteIds()) {
              TableName tname = TableName.fromString(tableWriteId.getFullTableName(), Warehouse.DEFAULT_CATALOG_NAME, Warehouse.DEFAULT_DATABASE_NAME);
              sharedCache.commitWriteId(tname.getCat(), tname.getDb(), tname.getTable(), tableWriteId.getWriteId());
            }
          }
          break;
      default:
        LOG.error("Event is not supported for cache invalidation : " + event.getEventType());
      }
    }
    return lastEventId;
  }

  @VisibleForTesting
  /**
   * This initializes the caches in SharedCache by getting the objects from Metastore DB via
   * ObjectStore and populating the respective caches
   */
  public static void prewarm(RawStore rawStore, Configuration conf) {
    if (isCachePrewarmed.get()) {
      return;
    }
    long startTime = System.nanoTime();
    LOG.info("Prewarming CachedStore");
    long sleepTime = 100;
    TxnStore txn = TxnUtils.getTxnStore(conf);
    while (!isCachePrewarmed.get()) {
      // Prevents throwing exceptions in our raw store calls since we're not using RawStoreProxy
      Deadline.registerIfNot(1000000);

      List<String> catNames = new ArrayList<>();
      try {
        catNames = rawStore.getCatalogs();
      } catch (MetaException e) {
        LOG.warn("Failed to get catalogs, going to try again", e);
        try {
          Thread.sleep(sleepTime);
          sleepTime = sleepTime * 2;
        } catch (InterruptedException timerEx) {
          LOG.info("sleep interrupted", timerEx.getMessage());
        }
        // try again
        continue;
      }

      List<Database> databases = new ArrayList<>();
      try {
        for (String catName : catNames) {
          List<String> dbNames = rawStore.getAllDatabases(catName);
          for (String dbName : dbNames) {
            try {
              databases.add(rawStore.getDatabase(catName, dbName));
            } catch (NoSuchObjectException e) {
              // Continue with next database
              LOG.warn("Failed to cache database " + DatabaseName.getQualified(catName, dbName) + ", moving on", e);
            }
          }
        }
      } catch (MetaException e) {
        LOG.warn("Failed to fetch databases, moving on", e);
      }
      LOG.info("Databases cache is now prewarmed. Now adding tables, partitions and statistics to the cache");
      int numberOfDatabasesCachedSoFar = 0;
      for (Database db : databases) {
        String catName = StringUtils.normalizeIdentifier(db.getCatalogName());
        String dbName = StringUtils.normalizeIdentifier(db.getName());
        List<String> tblNames;
        try {
          tblNames = rawStore.getAllTables(catName, dbName);
        } catch (MetaException e) {
          LOG.warn("Failed to cache tables for database " + DatabaseName.getQualified(catName, dbName) + ", moving on");
          // Continue with next database
          continue;
        }
        tblsPendingPrewarm.addTableNamesForPrewarming(tblNames);
        int totalTablesToCache = tblNames.size();
        int numberOfTablesCachedSoFar = 0;
        while (tblsPendingPrewarm.hasMoreTablesToPrewarm()) {
          try {
            String tblName = StringUtils.normalizeIdentifier(tblsPendingPrewarm.getNextTableNameToPrewarm());
            if (!shouldCacheTable(catName, dbName, tblName)) {
              continue;
            }
            Table table;
            ValidWriteIdList writeIds;
            try {
              ValidTxnList currentTxnList = TxnCommonUtils.createValidReadTxnList(txn.getOpenTxns(), 0);
              GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(Arrays.asList(TableName.getDbTable(dbName, tblName)));
              rqst.setValidTxnList(currentTxnList.toString());
              writeIds = TxnCommonUtils.createValidReaderWriteIdList(txn.getValidWriteIds(rqst).getTblValidWriteIds().get(0));
              table = rawStore.getTable(catName, dbName, tblName, null);
            } catch (MetaException e) {
              LOG.debug(ExceptionUtils.getStackTrace(e));
              // It is possible the table is deleted during fetching tables of the database,
              // in that case, continue with the next table
              continue;
            }  catch (NoSuchTxnException e) {
              LOG.warn("Cannot find transaction", e);
              continue;
            }
            List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
            try {
              ColumnStatistics tableColStats = null;
              List<Partition> partitions = null;
              List<ColumnStatistics> partitionColStats = null;
              AggrStats aggrStatsAllPartitions = null;
              AggrStats aggrStatsAllButDefaultPartition = null;
              if (!table.getPartitionKeys().isEmpty()) {
                Deadline.startTimer("getPartitions");
                partitions = rawStore.getPartitions(catName, dbName, tblName, -1, null);
                Deadline.stopTimer();
                List<String> partNames = new ArrayList<>(partitions.size());
                for (Partition p : partitions) {
                  partNames.add(Warehouse.makePartName(table.getPartitionKeys(), p.getValues()));
                }
                if (!partNames.isEmpty()) {
                  // Get partition column stats for this table
                  Deadline.startTimer("getPartitionColumnStatistics");
                  partitionColStats =
                      rawStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames, null);
                  Deadline.stopTimer();
                  // Get aggregate stats for all partitions of a table and for all but default
                  // partition
                  Deadline.startTimer("getAggrPartitionColumnStatistics");
                  aggrStatsAllPartitions = rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, null);
                  Deadline.stopTimer();
                  // Remove default partition from partition names and get aggregate
                  // stats again
                  List<FieldSchema> partKeys = table.getPartitionKeys();
                  String defaultPartitionValue =
                      MetastoreConf.getVar(rawStore.getConf(), ConfVars.DEFAULTPARTITIONNAME);
                  List<String> partCols = new ArrayList<>();
                  List<String> partVals = new ArrayList<>();
                  for (FieldSchema fs : partKeys) {
                    partCols.add(fs.getName());
                    partVals.add(defaultPartitionValue);
                  }
                  String defaultPartitionName = FileUtils.makePartName(partCols, partVals);
                  partNames.remove(defaultPartitionName);
                  Deadline.startTimer("getAggrPartitionColumnStatistics");
                  aggrStatsAllButDefaultPartition =
                      rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, null);
                  Deadline.stopTimer();
                }
              } else {
                Deadline.startTimer("getTableColumnStatistics");
                tableColStats = rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames, null);
                Deadline.stopTimer();
              }
              // If the table could not cached due to memory limit, stop prewarm
              boolean isSuccess = sharedCache
                  .populateTableInCache(table, tableColStats, partitions, partitionColStats, aggrStatsAllPartitions,
                      aggrStatsAllButDefaultPartition, writeIds);
              if (isSuccess) {
                LOG.trace("Cached Database: {}'s Table: {}.", dbName, tblName);
              } else {
                LOG.info("Unable to cache Database: {}'s Table: {}, since the cache memory is full. "
                    + "Will stop attempting to cache any more tables.", dbName, tblName);
                completePrewarm(startTime, false);
                return;
              }
            } catch (MetaException | NoSuchObjectException e) {
              LOG.debug(ExceptionUtils.getStackTrace(e));
              // Continue with next table
              continue;
            }
            LOG.debug("Processed database: {}'s table: {}. Cached {} / {}  tables so far.", dbName, tblName,
                ++numberOfTablesCachedSoFar, totalTablesToCache);
          } catch (Exception e) {
            LOG.debug(ExceptionUtils.getStackTrace(e));
            // skip table, continue with the next one
            continue;
          }
        }
        LOG.debug("Processed database: {}. Cached {} / {} databases so far.", dbName, ++numberOfDatabasesCachedSoFar,
            databases.size());
      }
      sharedCache.clearDirtyFlags();
      completePrewarm(startTime, true);
    }
  }

  /**
   * This method is only used for testing. Test method will init a new cache and use the new handle to query the cache
   * to get content in the cache. In production, no code would/should call this method, because SharedCache should be
   * a singleton.
   */
  @VisibleForTesting
  public static void clearSharedCache() {
    synchronized (lock) {
      sharedCacheInited = false;
    }
    sharedCache = new SharedCache();
    isCachePrewarmed.set(false);
  }

  static void completePrewarm(long startTime, boolean cachedAllMetadata) {
    isCachePrewarmed.set(true);
    isCachedAllMetadata.set(cachedAllMetadata);
    LOG.info("CachedStore initialized");
    long endTime = System.nanoTime();
    LOG.info("Time taken in prewarming = " + (endTime - startTime) / 1000000 + "ms");
    sharedCache.completeTableCachePrewarm();
  }

  static class TablesPendingPrewarm {
    private Stack<String> tableNames = new Stack<>();

    private synchronized void addTableNamesForPrewarming(List<String> tblNames) {
      tableNames.clear();
      if (tblNames != null) {
        tableNames.addAll(tblNames);
      }
    }

    private synchronized boolean hasMoreTablesToPrewarm() {
      return !tableNames.empty();
    }

    private synchronized String getNextTableNameToPrewarm() {
      return tableNames.pop();
    }

    private synchronized void prioritizeTableForPrewarm(String tblName) {
      // If the table is in the pending prewarm list, move it to the top
      if (tableNames.remove(tblName)) {
        tableNames.push(tblName);
      }
    }
  }

  @VisibleForTesting static void setCachePrewarmedState(boolean state) {
    isCachePrewarmed.set(state);
  }

  private static void initBlackListWhiteList(Configuration conf) {
    whitelistPatterns = createPatterns(
        MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST));
    blacklistPatterns = createPatterns(
        MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST));
  }

  @VisibleForTesting
  /**
   * This starts a background thread, which initially populates the SharedCache and later
   * periodically gets updates from the metastore db
   *
   * @param conf
   * @param runOnlyOnce
   * @param shouldRunPrewarm
   */ static void startCacheUpdateService(Configuration conf, boolean runOnlyOnce,
      boolean shouldRunPrewarm) {
    synchronized (startUpdateServiceLock) {
      if (cacheUpdateMaster == null) {
        initBlackListWhiteList(conf);
        if (!MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
          cacheRefreshPeriodMS =
              MetastoreConf.getTimeVar(conf, ConfVars.CACHED_RAW_STORE_CACHE_UPDATE_FREQUENCY, TimeUnit.MILLISECONDS);
        }
        LOG.info("CachedStore: starting cache update service (run every {} ms)", cacheRefreshPeriodMS);
        cacheUpdateMaster = Executors.newScheduledThreadPool(1, new ThreadFactory() {
          @Override public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("CachedStore-CacheUpdateService: Thread-" + t.getId());
            t.setDaemon(true);
            return t;
          }
        });
        if (!runOnlyOnce) {
          cacheUpdateMaster
              .scheduleAtFixedRate(new CacheUpdateMasterWork(conf, shouldRunPrewarm), 0, cacheRefreshPeriodMS,
                  TimeUnit.MILLISECONDS);
        }
      }
      if (runOnlyOnce) {
        // Some tests control the execution of the background update thread
        cacheUpdateMaster.schedule(new CacheUpdateMasterWork(conf, shouldRunPrewarm), 0, TimeUnit.MILLISECONDS);
      }
    }
  }

  @VisibleForTesting static synchronized boolean stopCacheUpdateService(long timeout) {
    boolean tasksStoppedBeforeShutdown = false;
    if (cacheUpdateMaster != null) {
      LOG.info("CachedStore: shutting down cache update service");
      try {
        tasksStoppedBeforeShutdown = cacheUpdateMaster.awaitTermination(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.info("CachedStore: cache update service was interrupted while waiting for tasks to "
            + "complete before shutting down. Will make a hard stop now.");
      }
      cacheUpdateMaster.shutdownNow();
      cacheUpdateMaster = null;
    }
    return tasksStoppedBeforeShutdown;
  }

  @VisibleForTesting static void setCacheRefreshPeriod(long time) {
    cacheRefreshPeriodMS = time;
  }

  static class CacheUpdateMasterWork implements Runnable {
    private boolean shouldRunPrewarm = true;
    private final RawStore rawStore;
    private Configuration conf;

    CacheUpdateMasterWork(Configuration conf, boolean shouldRunPrewarm) {
      this.shouldRunPrewarm = shouldRunPrewarm;
      this.conf = new Configuration(conf);
      String rawStoreClassName =
          MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL, ObjectStore.class.getName());
      try {
        rawStore = JavaUtils.getClass(rawStoreClassName, RawStore.class).newInstance();
        rawStore.setConf(this.conf);
      } catch (InstantiationException | IllegalAccessException | MetaException e) {
        // MetaException here really means ClassNotFound (see the utility method).
        // So, if any of these happen, that means we can never succeed.
        throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
      }
    }

    @Override public void run() {
      if (!shouldRunPrewarm) {
        try {
          triggerUpdateUsingEvent(rawStore, conf);
        } catch (Exception e) {
          LOG.error("failed to update cache using events ", e);
        }
        sharedCache.incrementUpdateCount();
      } else {
        try {
          triggerPreWarm(rawStore, conf);
          shouldRunPrewarm = false;
        } catch (Exception e) {
          LOG.error("Prewarm failure", e);
          return;
        }
      }
    }
  }

  @Override public Configuration getConf() {
    return rawStore.getConf();
  }

  @Override public void shutdown() {
    rawStore.shutdown();
  }

  @Override public boolean openTransaction() {
    return rawStore.openTransaction();
  }

  @Override public boolean commitTransaction() {
    return rawStore.commitTransaction();
  }

  @Override public boolean isActiveTransaction() {
    return rawStore.isActiveTransaction();
  }

  @Override public void rollbackTransaction() {
    rawStore.rollbackTransaction();
  }

  @Override public void createCatalog(Catalog cat) throws MetaException {
    rawStore.createCatalog(cat);
  }

  @Override public void alterCatalog(String catName, Catalog cat) throws MetaException, InvalidOperationException {
    rawStore.alterCatalog(catName, cat);
  }

  @Override public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    return rawStore.getCatalog(catalogName);
  }

  @Override public List<String> getCatalogs() throws MetaException {
    return rawStore.getCatalogs();
  }

  @Override public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    rawStore.dropCatalog(catalogName);
  }

  @Override public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    rawStore.createDatabase(db);
  }

  @Override public Database getDatabase(String catName, String dbName) throws NoSuchObjectException {
    return rawStore.getDatabase(catName, dbName);
  }

  @Override public boolean dropDatabase(String catName, String dbName) throws NoSuchObjectException, MetaException {
    return rawStore.dropDatabase(catName, dbName);
  }

  @Override public boolean alterDatabase(String catName, String dbName, Database db)
      throws NoSuchObjectException, MetaException {
    return rawStore.alterDatabase(catName, dbName, db);
  }

  @Override public List<String> getDatabases(String catName, String pattern) throws MetaException {
    return rawStore.getDatabases(catName, pattern);
  }

  @Override public List<String> getAllDatabases(String catName) throws MetaException {
    return rawStore.getAllDatabases(catName);
  }

  @Override public boolean createType(Type type) {
    return rawStore.createType(type);
  }

  @Override public Type getType(String typeName) {
    return rawStore.getType(typeName);
  }

  @Override public boolean dropType(String typeName) {
    return rawStore.dropType(typeName);
  }

  private void validateTableType(Table tbl) {
    // If the table has property EXTERNAL set, update table type
    // accordingly
    String tableType = tbl.getTableType();
    boolean isExternal = Boolean.parseBoolean(tbl.getParameters().get("EXTERNAL"));
    if (TableType.MANAGED_TABLE.toString().equals(tableType)) {
      if (isExternal) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      }
    }
    if (TableType.EXTERNAL_TABLE.toString().equals(tableType)) {
      if (!isExternal) {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }
    tbl.setTableType(tableType);
  }

  @Override public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    rawStore.createTable(tbl);
  }

  @Override public boolean dropTable(String catName, String dbName, String tblName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    return rawStore.dropTable(catName, dbName, tblName);
  }

  @Override public Table getTable(String catName, String dbName, String tblName, String validWriteIds)
      throws MetaException {
    catName = normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIds!=null?new ValidReaderWriteIdList(validWriteIds):null;
    if (writeIdsToRead == null || !shouldCacheTable(catName, dbName, tblName)) {
      if (writeIdsToRead == null) {
        LOG.debug("writeIdsToRead==null, read " + catName + "." + dbName + "." + tblName + " from db");
      }
      return rawStore.getTable(catName, dbName, tblName, validWriteIds);
    }
    Table tbl = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (tbl == null) {
      // no valid entry in cache
      // If the prewarm thread is working on this table's database,
      // let's move this table to the top of tblNamesBeingPrewarmed stack,
      // so that it gets loaded to the cache faster and is available for subsequent requests
      tblsPendingPrewarm.prioritizeTableForPrewarm(tblName);
      if (cacheMiss!=null) cacheMiss.inc();
      Table t = rawStore.getTable(catName, dbName, tblName, validWriteIds);
      if (t != null) {
        LOG.debug("cache miss, read " + catName + "." + dbName + "." + tblName + " from db");
      }
      return t;
    }

    if (!isTransactionalTable(tbl)) {
      LOG.debug("read " + catName + "." + dbName + "." + tblName + " from db since it is not transactional");
      return rawStore.getTable(catName, dbName, tblName, validWriteIds);
    }

    if (cacheHit!=null) cacheHit.inc();

    if (validWriteIds != null) {
      adjustStatsParamsForGet(tbl, validWriteIds);
    }

    tbl.unsetPrivileges();
    tbl.setRewriteEnabled(tbl.isRewriteEnabled());
    if (tbl.getPartitionKeys() == null) {
      // getTable call from ObjectStore returns an empty list
      tbl.setPartitionKeys(new ArrayList<>());
    }
    String tableType = tbl.getTableType();
    if (tableType == null) {
      // for backwards compatibility with old metastore persistence
      if (tbl.getViewOriginalText() != null) {
        tableType = TableType.VIRTUAL_VIEW.toString();
      } else if ("TRUE".equals(tbl.getParameters().get("EXTERNAL"))) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      } else {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }
    tbl.setTableType(tableType);
    return tbl;
  }

  @Override public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    return rawStore.addPartition(part);
  }

  @Override public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    return rawStore.addPartitions(catName, dbName, tblName, parts);
  }

  @Override public boolean addPartitions(String catName, String dbName, String tblName,
      PartitionSpecProxy partitionSpec, boolean ifNotExists) throws InvalidObjectException, MetaException {
    return rawStore.addPartitions(catName, dbName, tblName, partitionSpec, ifNotExists);
  }

  @Override public Partition getPartition(String catName, String dbName, String tblName, List<String> partVals,
      String validWriteIds) throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIds!=null?new ValidReaderWriteIdList(validWriteIds):null;
    if (writeIdsToRead == null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartition(catName, dbName, tblName, partVals, validWriteIds);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getPartition(catName, dbName, tblName, partVals, validWriteIds);
    }


    if (!isTransactionalTable(table)) {
      return rawStore.getPartition(catName, dbName, tblName, partVals, validWriteIds);
    }

    if (cacheHit!=null) cacheHit.inc();
    Partition part = sharedCache.getPartitionFromCache(catName, dbName, tblName, partVals);
    if (part == null) {
      // The table containing the partition is not yet loaded in cache
      return rawStore.getPartition(catName, dbName, tblName, partVals, validWriteIds);
    }
    if (validWriteIds != null) {
      adjustStatsParamsForGet(table, validWriteIds);
    }

    return part;
  }

  @Override public boolean doesPartitionExist(String catName, String dbName, String tblName, List<FieldSchema> partKeys,
      List<String> partVals, String validWriteIdList) throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (validWriteIdList == null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.doesPartitionExist(catName, dbName, tblName, partKeys, partVals, validWriteIdList);
    }
    Table tbl = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (tbl == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.doesPartitionExist(catName, dbName, tblName, partKeys, partVals, validWriteIdList);
    }

    if (!isTransactionalTable(tbl)) {
      return rawStore.doesPartitionExist(catName, dbName, tblName, partKeys, partVals, validWriteIdList);
    }

    return sharedCache.existPartitionFromCache(catName, dbName, tblName, partVals);
  }

  @Override public boolean dropPartition(String catName, String dbName, String tblName, List<String> partVals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    return rawStore.dropPartition(catName, dbName, tblName, partVals);
  }

  @Override public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    rawStore.dropPartitions(catName, dbName, tblName, partNames);
  }

  @Override public List<Partition> getPartitions(String catName, String dbName, String tblName, int max, String validWriteIdList)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitions(catName, dbName, tblName, max, validWriteIdList);
    }
    Table tbl = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (tbl == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getPartitions(catName, dbName, tblName, max, validWriteIdList);
    }

    if (!isTransactionalTable(tbl)) {
      return rawStore.getPartitions(catName, dbName, tblName, max, validWriteIdList);
    }

    if (cacheHit!=null) cacheHit.inc();
    List<Partition> parts = sharedCache.listCachedPartitions(catName, dbName, tblName, max);
    return parts;
  }

  @Override public Map<String, String> getPartitionLocations(String catName, String dbName, String tblName,
      String baseLocationToNotShow, int max, String validWriteIdList) {
    return rawStore.getPartitionLocations(catName, dbName, tblName, baseLocationToNotShow, max, validWriteIdList);
  }

  @Override public Table alterTable(String catName, String dbName, String tblName, Table newTable, String validWriteIds)
      throws InvalidObjectException, MetaException {
    return rawStore.alterTable(catName, dbName, tblName, newTable, validWriteIds);
  }

  @Override public void updateCreationMetadata(String catName, String dbname, String tablename, CreationMetadata cm)
      throws MetaException {
    rawStore.updateCreationMetadata(catName, dbname, tablename, cm);
  }

  @Override public List<String> getTables(String catName, String dbName, String pattern) throws MetaException {
    return rawStore.getTables(catName, dbName, pattern);
  }

  @Override public List<String> getTables(String catName, String dbName, String pattern, TableType tableType, int limit)
      throws MetaException {
    return rawStore.getTables(catName, dbName, pattern, tableType, limit);
  }

  @Override public List<Table> getAllMaterializedViewObjectsForRewriting(String catName) throws MetaException {
    // TODO fucntionCache
    return rawStore.getAllMaterializedViewObjectsForRewriting(catName);
  }

  @Override public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getMaterializedViewsForRewriting(catName, dbName);
  }

  @Override public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
      List<String> tableTypes) throws MetaException {
    return rawStore.getTableMeta(catName, dbNames, tableNames, tableTypes);
  }

  @Override public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tblNames)
      throws MetaException, UnknownDBException {
    return rawStore.getTableObjectsByName(catName, dbName, tblNames);
  }

  @Override public List<String> getAllTables(String catName, String dbName) throws MetaException {
    return rawStore.getAllTables(catName, dbName);
  }

  @Override
  // TODO: implement using SharedCache
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter, short maxTables)
      throws MetaException, UnknownDBException {
    return rawStore.listTableNamesByFilter(catName, dbName, filter, maxTables);
  }

  @Override public List<String> listPartitionNames(String catName, String dbName, String tblName, short maxParts, String validWriteIdList)
      throws MetaException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.listPartitionNames(catName, dbName, tblName, maxParts, validWriteIdList);
    }
    Table tbl = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (tbl == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.listPartitionNames(catName, dbName, tblName, maxParts, validWriteIdList);
    }

    if (!isTransactionalTable(tbl)) {
      return rawStore.listPartitionNames(catName, dbName, tblName, maxParts, validWriteIdList);
    }

    if (cacheHit!=null) cacheHit.inc();
    List<String> partitionNames = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(catName, dbName, tblName, maxParts)) {
      if (maxParts == -1 || count < maxParts) {
        partitionNames.add(Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
      }
    }
    return partitionNames;
  }

  @Override public PartitionValuesResponse listPartitionValues(String catName, String dbName, String tblName,
      List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending, List<FieldSchema> order,
      long maxParts, String validWriteIdList) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override public Partition alterPartition(String catName, String dbName, String tblName, List<String> partVals,
      Partition newPart, String validWriteIds) throws InvalidObjectException, MetaException {
    return rawStore.alterPartition(catName, dbName, tblName, partVals, newPart, validWriteIds);
  }

  @Override public List<Partition> alterPartitions(String catName, String dbName, String tblName,
      List<List<String>> partValsList, List<Partition> newParts, long writeId, String validWriteIds)
      throws InvalidObjectException, MetaException {
    return rawStore.alterPartitions(catName, dbName, tblName, partValsList, newParts, writeId, validWriteIds);
  }

  private boolean getPartitionNamesPrunedByExprNoTxn(Table table, byte[] expr, String defaultPartName, short maxParts,
      List<String> result, SharedCache sharedCache) throws MetaException, NoSuchObjectException {
    List<Partition> parts = sharedCache.listCachedPartitions(StringUtils.normalizeIdentifier(table.getCatName()),
        StringUtils.normalizeIdentifier(table.getDbName()), StringUtils.normalizeIdentifier(table.getTableName()),
        maxParts);
    for (Partition part : parts) {
      result.add(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(table.getPartitionKeys(), expr, defaultPartName, result);
  }

  @Override
  // TODO: implement using SharedCache
  public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName, String filter,
      short maxParts, String validWriteIdList) throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionsByFilter(catName, dbName, tblName, filter, maxParts, validWriteIdList);
  }

  @Override
  /**
   * getPartitionSpecsByFilterAndProjection interface is currently non-cacheable.
   */ public List<Partition> getPartitionSpecsByFilterAndProjection(Table table,
      GetPartitionsProjectionSpec projectionSpec, GetPartitionsFilterSpec filterSpec, String validWriteIdList)
      throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionSpecsByFilterAndProjection(table, projectionSpec, filterSpec, validWriteIdList);
  }

  @Override public boolean getPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result, String validWriteIdList) throws TException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName, maxParts, result, validWriteIdList);
    }
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName, maxParts, result, validWriteIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.getPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName, maxParts, result, validWriteIdList);
    }

    if (cacheHit!=null) cacheHit.inc();
    boolean hasUnknownPartitions =
        getPartitionNamesPrunedByExprNoTxn(table, expr, defaultPartitionName, maxParts, partNames, sharedCache);
    for (String partName : partNames) {
      Partition part = sharedCache.getPartitionFromCache(catName, dbName, tblName, partNameToVals(partName));
      part.unsetPrivileges();
      result.add(part);
    }
    return hasUnknownPartitions;
  }

  @Override public int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter, String validWriteIdList)
      throws MetaException, NoSuchObjectException {
    return rawStore.getNumPartitionsByFilter(catName, dbName, tblName, filter, validWriteIdList);
  }

  @Override public int getNumPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr, String validWriteIdList)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getNumPartitionsByExpr(catName, dbName, tblName, expr, validWriteIdList);
    }
    String defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getNumPartitionsByExpr(catName, dbName, tblName, expr, validWriteIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.getNumPartitionsByExpr(catName, dbName, tblName, expr, validWriteIdList);
    }

    getPartitionNamesPrunedByExprNoTxn(table, expr, defaultPartName, Short.MAX_VALUE, partNames, sharedCache);
    return partNames.size();
  }

  @VisibleForTesting public static List<String> partNameToVals(String name) {
    if (name == null) {
      return null;
    }
    List<String> vals = new ArrayList<>();
    String[] kvp = name.split("/");
    for (String kv : kvp) {
      vals.add(FileUtils.unescapePathName(kv.substring(kv.indexOf('=') + 1)));
    }
    return vals;
  }

  @Override public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      List<String> partNames, String validWriteIdList) throws MetaException, NoSuchObjectException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionsByNames(catName, dbName, tblName, partNames, validWriteIdList);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getPartitionsByNames(catName, dbName, tblName, partNames, validWriteIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.getPartitionsByNames(catName, dbName, tblName, partNames, validWriteIdList);
    }

    if (cacheHit!=null) cacheHit.inc();
    List<Partition> partitions = new ArrayList<>();
    for (String partName : partNames) {
      Partition part = sharedCache.getPartitionFromCache(catName, dbName, tblName, partNameToVals(partName));
      if (part != null) {
        partitions.add(part);
      }
    }
    return partitions;
  }

  @Override public Table markPartitionForEvent(String catName, String dbName, String tblName,
      Map<String, String> partVals, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    return rawStore.markPartitionForEvent(catName, dbName, tblName, partVals, evtType);
  }

  @Override public boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    return rawStore.isPartitionMarkedForEvent(catName, dbName, tblName, partName, evtType);
  }

  @Override public boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.addRole(rowName, ownerName);
  }

  @Override public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    return rawStore.removeRole(roleName);
  }

  @Override public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return rawStore.grantRole(role, userName, principalType, grantor, grantorType, grantOption);
  }

  @Override public boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException {
    return rawStore.revokeRole(role, userName, principalType, grantOption);
  }

  @Override public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return rawStore.getUserPrivilegeSet(userName, groupNames);
  }

  @Override public PrincipalPrivilegeSet getDBPrivilegeSet(String catName, String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getDBPrivilegeSet(catName, dbName, userName, groupNames);
  }

  @Override public PrincipalPrivilegeSet getTablePrivilegeSet(String catName, String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getTablePrivilegeSet(catName, dbName, tableName, userName, groupNames);
  }

  @Override public PrincipalPrivilegeSet getPartitionPrivilegeSet(String catName, String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getPartitionPrivilegeSet(catName, dbName, tableName, partition, userName, groupNames);
  }

  @Override public PrincipalPrivilegeSet getColumnPrivilegeSet(String catName, String dbName, String tableName,
      String partitionName, String columnName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return rawStore.getColumnPrivilegeSet(catName, dbName, tableName, partitionName, columnName, userName, groupNames);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType) {
    return rawStore.listPrincipalGlobalGrants(principalName, principalType);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName, PrincipalType principalType,
      String catName, String dbName) {
    return rawStore.listPrincipalDBGrants(principalName, principalType, catName, dbName);
  }

  @Override public List<HiveObjectPrivilege> listAllTableGrants(String principalName, PrincipalType principalType,
      String catName, String dbName, String tableName) {
    return rawStore.listAllTableGrants(principalName, principalType, catName, dbName, tableName);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName, List<String> partValues,
      String partName) {
    return rawStore
        .listPrincipalPartitionGrants(principalName, principalType, catName, dbName, tableName, partValues, partName);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName, String columnName) {
    return rawStore
        .listPrincipalTableColumnGrants(principalName, principalType, catName, dbName, tableName, columnName);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName, List<String> partValues,
      String partName, String columnName) {
    return rawStore
        .listPrincipalPartitionColumnGrants(principalName, principalType, catName, dbName, tableName, partValues,
            partName, columnName);
  }

  @Override public boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.grantPrivileges(privileges);
  }

  @Override public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.revokePrivileges(privileges, grantOption);
  }

  @Override public boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer,
      PrivilegeBag grantPrivileges) throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.refreshPrivileges(objToRefresh, authorizer, grantPrivileges);
  }

  @Override public Role getRole(String roleName) throws NoSuchObjectException {
    return rawStore.getRole(roleName);
  }

  @Override public List<String> listRoleNames() {
    return rawStore.listRoleNames();
  }

  @Override public List<Role> listRoles(String principalName, PrincipalType principalType) {
    return rawStore.listRoles(principalName, principalType);
  }

  @Override public List<RolePrincipalGrant> listRolesWithGrants(String principalName, PrincipalType principalType) {
    return rawStore.listRolesWithGrants(principalName, principalType);
  }

  @Override public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    return rawStore.listRoleMembers(roleName);
  }

  @Override public Partition getPartitionWithAuth(String catName, String dbName, String tblName, List<String> partVals,
      String userName, List<String> groupNames, String validWriteIdList) throws MetaException, NoSuchObjectException, InvalidObjectException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionWithAuth(catName, dbName, tblName, partVals, userName, groupNames, validWriteIdList);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getPartitionWithAuth(catName, dbName, tblName, partVals, userName, groupNames, validWriteIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.getPartitionWithAuth(catName, dbName, tblName, partVals, userName, groupNames, validWriteIdList);
    }

    if (cacheHit!=null) cacheHit.inc();
    Partition p = sharedCache.getPartitionFromCache(catName, dbName, tblName, partVals);
    if (p != null) {
      String partName = Warehouse.makePartName(table.getPartitionKeys(), partVals);
      PrincipalPrivilegeSet privs = getPartitionPrivilegeSet(catName, dbName, tblName, partName, userName, groupNames);
      p.setPrivileges(privs);
    } else {
      throw new NoSuchObjectException("partition values=" + partVals.toString());
    }
    return p;
  }

  @Override public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName, short maxParts,
      String userName, List<String> groupNames, String validWriteIdList) throws MetaException, NoSuchObjectException, InvalidObjectException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionsWithAuth(catName, dbName, tblName, maxParts, userName, groupNames, validWriteIdList);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getPartitionsWithAuth(catName, dbName, tblName, maxParts, userName, groupNames, validWriteIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.getPartitionsWithAuth(catName, dbName, tblName, maxParts, userName, groupNames, validWriteIdList);
    }

    if (cacheHit!=null) cacheHit.inc();
    List<Partition> partitions = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(catName, dbName, tblName, maxParts)) {
      if (maxParts == -1 || count < maxParts) {
        String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
        PrincipalPrivilegeSet privs =
            getPartitionPrivilegeSet(catName, dbName, tblName, partName, userName, groupNames);
        part.setPrivileges(privs);
        partitions.add(part);
        count++;
      }
    }
    return partitions;
  }

  @Override public List<String> listPartitionNamesPs(String catName, String dbName, String tblName,
      List<String> partSpecs, short maxParts, String validWriteIdList) throws MetaException, NoSuchObjectException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.listPartitionNamesPs(catName, dbName, tblName, partSpecs, maxParts, validWriteIdList);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.listPartitionNamesPs(catName, dbName, tblName, partSpecs, maxParts, validWriteIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.listPartitionNamesPs(catName, dbName, tblName, partSpecs, maxParts, validWriteIdList);
    }

    String partNameMatcher = getPartNameMatcher(table, partSpecs);
    List<String> partitionNames = new ArrayList<>();
    List<Partition> allPartitions = sharedCache.listCachedPartitions(catName, dbName, tblName, maxParts);
    int count = 0;
    for (Partition part : allPartitions) {
      String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
      if (partName.matches(partNameMatcher) && (maxParts == -1 || count < maxParts)) {
        partitionNames.add(partName);
        count++;
      }
    }
    return partitionNames;
  }

  @Override public List<Partition> listPartitionsPsWithAuth(String catName, String dbName, String tblName,
      List<String> partSpecs, short maxParts, String userName, List<String> groupNames, String validWriteIdList)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIdList!=null?new ValidReaderWriteIdList(validWriteIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.listPartitionsPsWithAuth(catName, dbName, tblName, partSpecs, maxParts, userName, groupNames, validWriteIdList);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.listPartitionsPsWithAuth(catName, dbName, tblName, partSpecs, maxParts, userName, groupNames, validWriteIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.listPartitionsPsWithAuth(catName, dbName, tblName, partSpecs, maxParts, userName, groupNames, validWriteIdList);
    }

    if (cacheHit!=null) cacheHit.inc();
    String partNameMatcher = getPartNameMatcher(table, partSpecs);
    List<Partition> partitions = new ArrayList<>();
    List<Partition> allPartitions = sharedCache.listCachedPartitions(catName, dbName, tblName, maxParts);
    int count = 0;
    for (Partition part : allPartitions) {
      String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
      if (partName.matches(partNameMatcher) && (maxParts == -1 || count < maxParts)) {
        PrincipalPrivilegeSet privs =
            getPartitionPrivilegeSet(catName, dbName, tblName, partName, userName, groupNames);
        part.setPrivileges(privs);
        partitions.add(part);
        count++;
      }
    }
    return partitions;
  }

  private String getPartNameMatcher(Table table, List<String> partSpecs) throws MetaException {
    List<FieldSchema> partCols = table.getPartitionKeys();
    int numPartKeys = partCols.size();
    if (partSpecs.size() > numPartKeys) {
      throw new MetaException(
          "Incorrect number of partition values." + " numPartKeys=" + numPartKeys + ", partSpecs=" + partSpecs.size());
    }
    partCols = partCols.subList(0, partSpecs.size());
    // Construct a pattern of the form: partKey=partVal/partKey2=partVal2/...
    // where partVal is either the escaped partition value given as input,
    // or a regex of the form ".*"
    // This works because the "=" and "/" separating key names and partition key/values
    // are not escaped.
    String partNameMatcher = Warehouse.makePartName(partCols, partSpecs, ".*");
    // add ".*" to the regex to match anything else afterwards the partial spec.
    if (partSpecs.size() < numPartKeys) {
      partNameMatcher += ".*";
    }
    return partNameMatcher;
  }

  // Note: ideally this should be above both CachedStore and ObjectStore.
  private void adjustStatsParamsForGet(Table tbl, String validWriteIds) throws MetaException {
    boolean isTxn = tbl != null && TxnUtils.isTransactionalTable(tbl);
    if (isTxn && !areTxnStatsSupported) {
      StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
      LOG.info("Removed COLUMN_STATS_ACCURATE from Table's parameters.");
    } else if (isTxn && tbl.getPartitionKeysSize() == 0) {
      if (ObjectStore.isCurrentStatsValidForTheQuery(tbl.getParameters(), tbl.getWriteId(), validWriteIds, false)) {
        tbl.setIsStatsCompliant(true);
      } else {
        tbl.setIsStatsCompliant(false);
        // Do not make persistent the following state since it is the query specific (not global).
        StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
        LOG.info("Removed COLUMN_STATS_ACCURATE from Table's parameters.");
      }
    }
  }

  // Note: ideally this should be above both CachedStore and ObjectStore.
  public static ColumnStatistics adjustColStatForGet(Map<String, String> tableParams, ColumnStatistics colStat,
      long statsWriteId, String validWriteIds, boolean areTxnStatsSupported) throws MetaException {
    colStat.setIsStatsCompliant(true);
    if (!TxnUtils.isTransactionalTable(tableParams)) {
      return colStat; // Not a txn table.
    }
    if (areTxnStatsSupported && ((validWriteIds == null) || ObjectStore
        .isCurrentStatsValidForTheQuery(tableParams, statsWriteId, validWriteIds, false))) {
      // Valid stats are supported for txn tables, and either no verification was requested by the
      // caller, or the verification has succeeded.
      return colStat;
    }
    // Don't clone; ColStats objects are not cached, only their parts.
    colStat.setIsStatsCompliant(false);
    return colStat;
  }

  @Override public Map<String, String> updateTableColumnStatistics(ColumnStatistics colStats, String validWriteIds,
      long writeId) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return rawStore.updateTableColumnStatistics(colStats, validWriteIds, writeId);
  }

  @Override public ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tblName,
      List<String> colNames, String validWriteIds) throws MetaException, NoSuchObjectException {
    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = validWriteIds!=null?new ValidReaderWriteIdList(validWriteIds):null;
    if (validWriteIds==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames, validWriteIds);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames, validWriteIds);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames, validWriteIds);
    }

    if (cacheHit!=null) cacheHit.inc();
    ColumnStatistics columnStatistics =
        sharedCache.getTableColStatsFromCache(catName, dbName, tblName, colNames, validWriteIds, areTxnStatsSupported);
    if (columnStatistics == null) {
      LOG.info("Stat of Table {}.{} for column {} is not present in cache." + "Getting from raw store", dbName, tblName,
          colNames);
      return rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames, validWriteIds);
    }
    return columnStatistics;
  }

  @Override public boolean deleteTableColumnStatistics(String catName, String dbName, String tblName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return rawStore.deleteTableColumnStatistics(catName, dbName, tblName, colName);
  }

  @Override public Map<String, String> updatePartitionColumnStatistics(ColumnStatistics colStats, List<String> partVals,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return rawStore.updatePartitionColumnStatistics(colStats, partVals, validWriteIds, writeId);
  }

  @Override public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames, String writeIdList) throws MetaException, NoSuchObjectException {

    catName = StringUtils.normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = writeIdList!=null?new ValidReaderWriteIdList(writeIdList):null;
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames, writeIdList);
    }

    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames, writeIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames, writeIdList);
    }

    if (cacheHit!=null) cacheHit.inc();

    // If writeIdList is not null, that means stats are requested within a txn context. So set stats compliant to false,
    // if areTxnStatsSupported is false or the write id which has updated the stats in not compatible with writeIdList.
    // This is done within table lock as the number of partitions may be more than one and we need a consistent view
    // for all the partitions.
    List<ColumnStatistics> columnStatistics = sharedCache
        .getPartitionColStatsListFromCache(catName, dbName, tblName, partNames, colNames, writeIdList,
            areTxnStatsSupported);
    if (columnStatistics == null) {
      return rawStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames, writeIdList);
    }
    return columnStatistics;
  }

  @Override public boolean deletePartitionColumnStatistics(String catName, String dbName, String tblName,
      String partName, List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return rawStore.deletePartitionColumnStatistics(catName, dbName, tblName, partName, partVals, colName);
  }

  @Override public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName, List<String> partNames,
      List<String> colNames, String writeIdList) throws MetaException, NoSuchObjectException {
    List<ColumnStatisticsObj> colStats;
    catName = normalizeIdentifier(catName);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    ValidWriteIdList writeIdsToRead = writeIdList!=null?new ValidReaderWriteIdList(writeIdList):null;
    // TODO: we currently cannot do transactional checks for stats here
    //       (incl. due to lack of sync w.r.t. the below rawStore call).
    // In case the cache is updated using events, aggregate is calculated locally and thus can be read from cache.
    if (writeIdsToRead==null || !shouldCacheTable(catName, dbName, tblName) || writeIdList != null) {
      return rawStore.get_aggr_stats_for(
          catName, dbName, tblName, partNames, colNames, writeIdList);
    }
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName, writeIdsToRead);

    if (table == null) {
      // no valid entry in cache
      if (cacheMiss!=null) cacheMiss.inc();
      return rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, writeIdList);
    }

    if (!isTransactionalTable(table)) {
      return rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, writeIdList);
    }

    List<String> allPartNames = rawStore.listPartitionNames(catName, dbName, tblName, (short) -1, writeIdList);
    StatsType type = StatsType.PARTIAL;
    if (partNames.size() == allPartNames.size()) {
      colStats = sharedCache.getAggrStatsFromCache(catName, dbName, tblName, colNames, StatsType.ALL);
      if (colStats != null) {
        return new AggrStats(colStats, partNames.size());
      }
      type = StatsType.ALL;
    } else if (partNames.size() == (allPartNames.size() - 1)) {
      String defaultPartitionName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
      if (!partNames.contains(defaultPartitionName)) {
        colStats = sharedCache.getAggrStatsFromCache(catName, dbName, tblName, colNames, StatsType.ALLBUTDEFAULT);
        if (colStats != null) {
          return new AggrStats(colStats, partNames.size());
        }
        type = StatsType.ALLBUTDEFAULT;
      }
    }

    LOG.debug("Didn't find aggr stats in cache. Merging them. tblName= {}, parts= {}, cols= {}", tblName, partNames,
        colNames);
    MergedColumnStatsForPartitions mergedColStats =
        mergeColStatsForPartitions(catName, dbName, tblName, partNames, colNames, sharedCache, type, writeIdList,
        MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_NDV_DENSITY_FUNCTION),
        MetastoreConf.getDoubleVar(getConf(), ConfVars.STATS_NDV_TUNER));
    if (mergedColStats == null) {
      LOG.info("Aggregate stats of partition " + TableName.getQualified(catName, dbName, tblName) + "." + partNames
          + " for columns " + colNames + " is not present in cache. Getting it from raw store");
      return rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, writeIdList);
    }
    return new AggrStats(mergedColStats.getColStats(), mergedColStats.getPartsFound());
  }

  static MergedColumnStatsForPartitions mergeColStatsForPartitions(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames, SharedCache sharedCache, StatsType type, String writeIdList,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner)
      throws MetaException {
    Map<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> colStatsMap = new HashMap<>();
    long partsFound = partNames.size();
    Map<List<String>, Long> partNameToWriteId = writeIdList != null ? new HashMap<>() : null;
    for (String colName : colNames) {
      long partsFoundForColumn = 0;
      ColumnStatsAggregator colStatsAggregator = null;
      List<ColStatsObjWithSourceInfo> colStatsWithPartInfoList = new ArrayList<>();
      for (String partName : partNames) {
        List<String> partValue = partNameToVals(partName);
        // There are three possible result from getPartitionColStatsFromCache.
        // 1. The partition has valid stats and thus colStatsWriteId returned is valid non-null value
        // 2. Partition stat is missing from cache and thus colStatsWriteId returned is non-null but colstat
        //    info in it is null. In this case we just ignore the partition from aggregate calculation to keep
        //    the behavior same as object store.
        // 3. Partition is missing or its stat is updated by live(not yet committed) or aborted txn. In this case,
        //    colStatsWriteId is null. Thus null is returned to keep the behavior same as object store.
        SharedCache.ColumStatsWithWriteId colStatsWriteId =
            sharedCache.getPartitionColStatsFromCache(catName, dbName, tblName, partValue, colName, writeIdList);
        if (colStatsWriteId == null) {
          return null;
        }
        if (colStatsWriteId.getColumnStatisticsObj() != null) {
          ColumnStatisticsObj colStatsForPart = colStatsWriteId.getColumnStatisticsObj();
          if (partNameToWriteId != null) {
            partNameToWriteId.put(partValue, colStatsWriteId.getWriteId());
          }
          ColStatsObjWithSourceInfo colStatsWithPartInfo =
              new ColStatsObjWithSourceInfo(colStatsForPart, catName, dbName, tblName, partName);
          colStatsWithPartInfoList.add(colStatsWithPartInfo);
          if (colStatsAggregator == null) {
            colStatsAggregator = ColumnStatsAggregatorFactory
                .getColumnStatsAggregator(colStatsForPart.getStatsData().getSetField(),
                    useDensityFunctionForNDVEstimation, ndvTuner);
          }
          partsFoundForColumn++;
        } else {
          LOG.debug("Stats not found in CachedStore for: dbName={} tblName={} partName={} colName={}", dbName, tblName,
              partName, colName);
        }
      }
      if (colStatsWithPartInfoList.size() > 0) {
        colStatsMap.put(colStatsAggregator, colStatsWithPartInfoList);
      }
      // set partsFound to the min(partsFoundForColumn) for all columns. partsFound is the number of partitions, for
      // which stats for all columns are present in the cache.
      if (partsFoundForColumn < partsFound) {
        partsFound = partsFoundForColumn;
      }
      if (colStatsMap.size() < 1) {
        LOG.debug("No stats data found for: dbName={} tblName= {} partNames= {} colNames= ", dbName, tblName, partNames,
            colNames);
        return new MergedColumnStatsForPartitions(new ArrayList<ColumnStatisticsObj>(), 0);
      }
    }
    // Note that enableBitVector does not apply here because ColumnStatisticsObj
    // itself will tell whether bitvector is null or not and aggr logic can automatically apply.
    List<ColumnStatisticsObj> colAggrStats = MetaStoreServerUtils
        .aggrPartitionStats(colStatsMap, partNames, partsFound == partNames.size(), useDensityFunctionForNDVEstimation,
            ndvTuner);

    if (type == StatsType.ALL) {
      sharedCache.refreshAggregateStatsInCache(StringUtils.normalizeIdentifier(catName),
          StringUtils.normalizeIdentifier(dbName), StringUtils.normalizeIdentifier(tblName),
          new AggrStats(colAggrStats, partsFound), null, partNameToWriteId);
    } else if (type == StatsType.ALLBUTDEFAULT) {
      sharedCache.refreshAggregateStatsInCache(StringUtils.normalizeIdentifier(catName),
          StringUtils.normalizeIdentifier(dbName), StringUtils.normalizeIdentifier(tblName), null,
          new AggrStats(colAggrStats, partsFound), partNameToWriteId);
    }
    return new MergedColumnStatsForPartitions(colAggrStats, partsFound);
  }

  static class MergedColumnStatsForPartitions {
    List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>();
    long partsFound;

    MergedColumnStatsForPartitions(List<ColumnStatisticsObj> colStats, long partsFound) {
      this.colStats = colStats;
      this.partsFound = partsFound;
    }

    List<ColumnStatisticsObj> getColStats() {
      return colStats;
    }

    long getPartsFound() {
      return partsFound;
    }
  }

  @Override public long cleanupEvents() {
    return rawStore.cleanupEvents();
  }

  @Override public boolean addToken(String tokenIdentifier, String delegationToken) {
    return rawStore.addToken(tokenIdentifier, delegationToken);
  }

  @Override public boolean removeToken(String tokenIdentifier) {
    return rawStore.removeToken(tokenIdentifier);
  }

  @Override public String getToken(String tokenIdentifier) {
    return rawStore.getToken(tokenIdentifier);
  }

  @Override public List<String> getAllTokenIdentifiers() {
    return rawStore.getAllTokenIdentifiers();
  }

  @Override public int addMasterKey(String key) throws MetaException {
    return rawStore.addMasterKey(key);
  }

  @Override public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException {
    rawStore.updateMasterKey(seqNo, key);
  }

  @Override public boolean removeMasterKey(Integer keySeq) {
    return rawStore.removeMasterKey(keySeq);
  }

  @Override public String[] getMasterKeys() {
    return rawStore.getMasterKeys();
  }

  @Override public void verifySchema() throws MetaException {
    rawStore.verifySchema();
  }

  @Override public String getMetaStoreSchemaVersion() throws MetaException {
    return rawStore.getMetaStoreSchemaVersion();
  }

  @Override public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
    rawStore.setMetaStoreSchemaVersion(version, comment);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName,
      PrincipalType principalType) {
    return rawStore.listPrincipalDBGrantsAll(principalName, principalType);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
      PrincipalType principalType) {
    return rawStore.listPrincipalTableGrantsAll(principalName, principalType);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
      PrincipalType principalType) {
    return rawStore.listPrincipalPartitionGrantsAll(principalName, principalType);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    return rawStore.listPrincipalTableColumnGrantsAll(principalName, principalType);
  }

  @Override public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    return rawStore.listPrincipalPartitionColumnGrantsAll(principalName, principalType);
  }

  @Override public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    return rawStore.listGlobalGrantsAll();
  }

  @Override public List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName) {
    return rawStore.listDBGrantsAll(catName, dbName);
  }

  @Override public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String catName, String dbName,
      String tableName, String partitionName, String columnName) {
    return rawStore.listPartitionColumnGrantsAll(catName, dbName, tableName, partitionName, columnName);
  }

  @Override public List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName, String tableName) {
    return rawStore.listTableGrantsAll(catName, dbName, tableName);
  }

  @Override public List<HiveObjectPrivilege> listPartitionGrantsAll(String catName, String dbName, String tableName,
      String partitionName) {
    return rawStore.listPartitionGrantsAll(catName, dbName, tableName, partitionName);
  }

  @Override public List<HiveObjectPrivilege> listTableColumnGrantsAll(String catName, String dbName, String tableName,
      String columnName) {
    return rawStore.listTableColumnGrantsAll(catName, dbName, tableName, columnName);
  }

  @Override public void createFunction(Function func) throws InvalidObjectException, MetaException {
    // TODO fucntionCache
    rawStore.createFunction(func);
  }

  @Override public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
    // TODO fucntionCache
    rawStore.alterFunction(catName, dbName, funcName, newFunction);
  }

  @Override public void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    // TODO fucntionCache
    rawStore.dropFunction(catName, dbName, funcName);
  }

  @Override public Function getFunction(String catName, String dbName, String funcName) throws MetaException {
    // TODO fucntionCache
    return rawStore.getFunction(catName, dbName, funcName);
  }

  @Override public List<Function> getAllFunctions(String catName) throws MetaException {
    // TODO fucntionCache
    return rawStore.getAllFunctions(catName);
  }

  @Override public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException {
    // TODO fucntionCache
    return rawStore.getFunctions(catName, dbName, pattern);
  }

  @Override public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    return rawStore.getNextNotification(rqst);
  }

  @Override public void addNotificationEvent(NotificationEvent event) throws MetaException {
    rawStore.addNotificationEvent(event);
  }

  @Override public void cleanNotificationEvents(int olderThan) {
    rawStore.cleanNotificationEvents(olderThan);
  }

  @Override public CurrentNotificationEventId getCurrentNotificationEventId() {
    return rawStore.getCurrentNotificationEventId();
  }

  @Override public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    return rawStore.getNotificationEventsCount(rqst);
  }

  @Override public void flushCache() {
    rawStore.flushCache();
  }

  @Override public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    return rawStore.getFileMetadata(fileIds);
  }

  @Override public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata, FileMetadataExprType type)
      throws MetaException {
    rawStore.putFileMetadata(fileIds, metadata, type);
  }

  @Override public boolean isFileMetadataSupported() {
    return rawStore.isFileMetadataSupported();
  }

  @Override public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] exprResults, boolean[] eliminated) throws MetaException {
    rawStore.getFileMetadataByExpr(fileIds, type, expr, metadatas, exprResults, eliminated);
  }

  @Override public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    return rawStore.getFileMetadataHandler(type);
  }

  @Override public int getTableCount() throws MetaException {
    return rawStore.getTableCount();
  }

  @Override public int getPartitionCount() throws MetaException {
    return rawStore.getPartitionCount();
  }

  @Override public int getDatabaseCount() throws MetaException {
    return rawStore.getDatabaseCount();
  }

  @Override public List<SQLPrimaryKey> getPrimaryKeys(String catName, String dbName, String tblName)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getPrimaryKeys(catName, dbName, tblName);
  }

  @Override public List<SQLForeignKey> getForeignKeys(String catName, String parentDbName, String parentTblName,
      String foreignDbName, String foreignTblName) throws MetaException {
    // TODO constraintCache
    return rawStore.getForeignKeys(catName, parentDbName, parentTblName, foreignDbName, foreignTblName);
  }

  @Override public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String dbName, String tblName)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getUniqueConstraints(catName, dbName, tblName);
  }

  @Override public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String dbName, String tblName)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getNotNullConstraints(catName, dbName, tblName);
  }

  @Override public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String dbName, String tblName)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getDefaultConstraints(catName, dbName, tblName);
  }

  @Override public List<SQLCheckConstraint> getCheckConstraints(String catName, String dbName, String tblName)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getCheckConstraints(catName, dbName, tblName);
  }

  @Override public List<String> createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints) throws InvalidObjectException, MetaException {
    return rawStore.createTableWithConstraints(tbl, primaryKeys,
        foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
  }

  @Override public void dropConstraint(String catName, String dbName, String tableName, String constraintName,
      boolean missingOk) throws NoSuchObjectException {
    // TODO constraintCache
    rawStore.dropConstraint(catName, dbName, tableName, constraintName, missingOk);
  }

  @Override public List<String> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addPrimaryKeys(pks);
  }

  @Override public List<String> addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addForeignKeys(fks);
  }

  @Override public List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addUniqueConstraints(uks);
  }

  @Override public List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addNotNullConstraints(nns);
  }

  @Override public List<String> addDefaultConstraints(List<SQLDefaultConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addDefaultConstraints(nns);
  }

  @Override public List<String> addCheckConstraints(List<SQLCheckConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addCheckConstraints(nns);
  }

  // TODO - not clear if we should cache these or not.  For now, don't bother
  @Override public void createISchema(ISchema schema)
      throws AlreadyExistsException, NoSuchObjectException, MetaException {
    rawStore.createISchema(schema);
  }

  @Override public List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionColStatsForDatabase(catName, dbName);
  }

  @Override public void alterISchema(ISchemaName schemaName, ISchema newSchema)
      throws NoSuchObjectException, MetaException {
    rawStore.alterISchema(schemaName, newSchema);
  }

  @Override public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    return rawStore.getISchema(schemaName);
  }

  @Override public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {
    rawStore.dropISchema(schemaName);
  }

  @Override public void addSchemaVersion(SchemaVersion schemaVersion)
      throws AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {
    rawStore.addSchemaVersion(schemaVersion);
  }

  @Override public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion)
      throws NoSuchObjectException, MetaException {
    rawStore.alterSchemaVersion(version, newVersion);
  }

  @Override public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    return rawStore.getSchemaVersion(version);
  }

  @Override public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    return rawStore.getLatestSchemaVersion(schemaName);
  }

  @Override public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    return rawStore.getAllSchemaVersion(schemaName);
  }

  @Override public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace, String type)
      throws MetaException {
    return rawStore.getSchemaVersionsByColumns(colName, colNamespace, type);
  }

  @Override public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException, MetaException {
    rawStore.dropSchemaVersion(version);
  }

  @Override public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
    return rawStore.getSerDeInfo(serDeName);
  }

  @Override public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {
    rawStore.addSerde(serde);
  }

  public RawStore getRawStore() {
    return rawStore;
  }

  @VisibleForTesting public void setRawStore(RawStore rawStore) {
    this.rawStore = rawStore;
  }

  @Override public String getMetastoreDbUuid() throws MetaException {
    return rawStore.getMetastoreDbUuid();
  }

  @Override public void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException {
    rawStore.createResourcePlan(resourcePlan, copyFrom, defaultPoolSize);
  }

  @Override public WMFullResourcePlan getResourcePlan(String name, String ns)
      throws NoSuchObjectException, MetaException {
    return rawStore.getResourcePlan(name, ns);
  }

  @Override public List<WMResourcePlan> getAllResourcePlans(String ns) throws MetaException {
    return rawStore.getAllResourcePlans(ns);
  }

  @Override public WMFullResourcePlan alterResourcePlan(String name, String ns, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    return rawStore.alterResourcePlan(name, ns, resourcePlan, canActivateDisabled, canDeactivate, isReplace);
  }

  @Override public WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException {
    return rawStore.getActiveResourcePlan(ns);
  }

  @Override public WMValidateResourcePlanResponse validateResourcePlan(String name, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    return rawStore.validateResourcePlan(name, ns);
  }

  @Override public void dropResourcePlan(String name, String ns) throws NoSuchObjectException, MetaException {
    rawStore.dropResourcePlan(name, ns);
  }

  @Override public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, MetaException, NoSuchObjectException, InvalidOperationException {
    rawStore.createWMTrigger(trigger);
  }

  @Override public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.alterWMTrigger(trigger);
  }

  @Override public void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMTrigger(resourcePlanName, triggerName, ns);
  }

  @Override public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException {
    return rawStore.getTriggersForResourcePlan(resourcePlanName, ns);
  }

  @Override public void createPool(WMPool pool)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.createPool(pool);
  }

  @Override public void alterPool(WMNullablePool pool, String poolPath)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.alterPool(pool, poolPath);
  }

  @Override public void dropWMPool(String resourcePlanName, String poolPath, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMPool(resourcePlanName, poolPath, ns);
  }

  @Override public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.createOrUpdateWMMapping(mapping, update);
  }

  @Override public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMMapping(mapping);
  }

  @Override public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath,
      String ns) throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.createWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath, ns);
  }

  @Override public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath,
      String ns) throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath, ns);
  }

  public long getCacheUpdateCount() {
    return sharedCache.getUpdateCount();
  }

  @Override public void cleanWriteNotificationEvents(int olderThan) {
    rawStore.cleanWriteNotificationEvents(olderThan);
  }

  @Override public List<WriteEventInfo> getAllWriteEventInfo(long txnId, String dbName, String tableName)
      throws MetaException {
    return rawStore.getAllWriteEventInfo(txnId, dbName, tableName);
  }

  static boolean isNotInBlackList(String catName, String dbName, String tblName) {
    String str = TableName.getQualified(catName, dbName, tblName);
    for (Pattern pattern : blacklistPatterns) {
      LOG.debug("Trying to match: {} against blacklist pattern: {}", str, pattern);
      Matcher matcher = pattern.matcher(str);
      if (matcher.matches()) {
        LOG.debug("Found matcher group: {} at start index: {} and end index: {}", matcher.group(), matcher.start(),
            matcher.end());
        return false;
      }
    }
    return true;
  }

  private static boolean isInWhitelist(String catName, String dbName, String tblName) {
    String str = TableName.getQualified(catName, dbName, tblName);
    for (Pattern pattern : whitelistPatterns) {
      LOG.debug("Trying to match: {} against whitelist pattern: {}", str, pattern);
      Matcher matcher = pattern.matcher(str);
      if (matcher.matches()) {
        LOG.debug("Found matcher group: {} at start index: {} and end index: {}", matcher.group(), matcher.start(),
            matcher.end());
        return true;
      }
    }
    return false;
  }

  // For testing
  static void setWhitelistPattern(List<Pattern> patterns) {
    whitelistPatterns = patterns;
  }

  // For testing
  static void setBlacklistPattern(List<Pattern> patterns) {
    blacklistPatterns = patterns;
  }

  // Determines if we should cache a table (& its partitions, stats etc),
  // based on whitelist/blacklist
  static boolean shouldCacheTable(String catName, String dbName, String tblName) {
    if (!isNotInBlackList(catName, dbName, tblName)) {
      LOG.debug("{}.{} is in blacklist, skipping", dbName, tblName);
      return false;
    }
    if (!isInWhitelist(catName, dbName, tblName)) {
      LOG.debug("{}.{} is not in whitelist, skipping", dbName, tblName);
      return false;
    }
    return true;
  }

  static List<Pattern> createPatterns(String configStr) {
    List<String> patternStrs = Arrays.asList(configStr.split(","));
    List<Pattern> patterns = new ArrayList<Pattern>();
    for (String str : patternStrs) {
      patterns.add(Pattern.compile(str));
    }
    return patterns;
  }

  static boolean isBlacklistWhitelistEmpty(Configuration conf) {
    return
        MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST).equals(".*")
            && MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST)
            .isEmpty();
  }

  @Override public void addRuntimeStat(RuntimeStat stat) throws MetaException {
    rawStore.addRuntimeStat(stat);
  }

  @Override public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
    return rawStore.getRuntimeStats(maxEntries, maxCreateTime);
  }

  @Override public int deleteRuntimeStats(int maxRetainSecs) throws MetaException {
    return rawStore.deleteRuntimeStats(maxRetainSecs);
  }

  @Override public List<TableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
    return rawStore.getTableNamesWithStats();
  }

  @Override public List<TableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
    return rawStore.getAllTableNamesForStats();
  }

  @Override public Map<String, List<String>> getPartitionColsWithStats(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionColsWithStats(catName, dbName, tableName);
  }

  public static boolean isTransactionalTable(org.apache.hadoop.hive.metastore.api.Table table) {
    return table != null && table.getParameters() != null &&
        isTablePropertyTransactional(table.getParameters());
  }

  public static boolean isTablePropertyTransactional(Map<String, String> parameters) {
    String resultStr = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    if (resultStr == null) {
      resultStr = parameters.get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return resultStr != null && resultStr.equalsIgnoreCase("true");
  }

  public static ValidWriteIdList newTableWriteIds(String dbName, String tableName) {
    String fullTableName = TableName.getDbTable(dbName, tableName);
    return new ValidReaderWriteIdList(fullTableName, new long[]{1}, new BitSet(), 1);
  }
}
