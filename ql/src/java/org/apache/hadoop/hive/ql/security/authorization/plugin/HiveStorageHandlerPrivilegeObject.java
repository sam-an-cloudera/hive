package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

import java.util.List;

public class HiveStorageHandlerPrivilegeObject extends HivePrivilegeObject {
  protected final HiveStorageHandler storageHandler;
  protected final Table tableMetadata;
  public enum StoragePrivilege{
    CREATE,
    READ,
    UPDATE,
    DELETE
  };

  public HiveStorageHandlerPrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      List<String> partKeys, List<String> columns, HivePrivObjectActionType actionType, List<String> commandParams,
      String className, String ownerName, PrincipalType ownerType, Table tableMetadata,
      HiveStorageHandler storageHandler) {
    super(type, dbname, objectName, partKeys, columns, actionType, commandParams, className, ownerName, ownerType);
    this.tableMetadata = tableMetadata;
    this.storageHandler = storageHandler;
  }

  public void authorizeAction(StoragePrivilege privsRequested) throws HiveException {
    throw new HiveException("Not implemented for this storage handler");
  }

  //ranger
}
