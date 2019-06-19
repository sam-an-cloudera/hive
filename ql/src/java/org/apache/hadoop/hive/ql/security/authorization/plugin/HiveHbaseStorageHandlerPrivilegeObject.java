package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

import java.util.List;

public class HiveHbaseStorageHandlerPrivilegeObject extends HivePrivilegeObject {
  private final HiveStorageHandler storageHandler;
  private final Table tableMetadata;
  public HiveHbaseStorageHandlerPrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      List<String> partKeys, List<String> columns, HivePrivObjectActionType actionType, List<String> commandParams,
      String className, Table tableMetadata,
      HiveStorageHandler storageHandler) {
    super(type, dbname, objectName, partKeys, columns, actionType, commandParams, className);
    this.tableMetadata = tableMetadata;
    this.storageHandler = storageHandler;
  }


  public void authorizeAction(HiveOperationType opType) throws HiveException {
    HiveAuthorizationProvider authProvider = storageHandler.getAuthorizationProvider();
    if (authProvider != null){
      Privilege[] readPrivileges = new Privilege[1];
      if (opType == HiveOperationType.CREATETABLE){
        readPrivileges[0] = Privilege.CREATE;
      }
      Privilege[] writePrivileges= null;
      authProvider.authorize(this.tableMetadata, readPrivileges, writePrivileges);
    }
  }
}
