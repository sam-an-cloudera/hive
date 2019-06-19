package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

import java.util.List;

public class HiveStorageHandlerPrivilegeObject extends HivePrivilegeObject {
  protected final HiveStorageHandler storageHandler;
  protected final Table tableMetadata;
  public HiveStorageHandlerPrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      List<String> partKeys, List<String> columns, HivePrivObjectActionType actionType, List<String> commandParams,
      String className, Table tableMetadata,
      HiveStorageHandler storageHandler) {
    super(type, dbname, objectName, partKeys, columns, actionType, commandParams, className);
    this.tableMetadata = tableMetadata;
    this.storageHandler = storageHandler;
  }

  public void authorizeAction(HiveOperationType opType) throws HiveException {
    throw new HiveException("Not implemented for this storage handler");
  }

}
