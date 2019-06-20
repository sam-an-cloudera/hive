package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

import java.util.List;

public class HiveHbaseStorageHandlerPrivilegeObject extends HiveStorageHandlerPrivilegeObject {
  public HiveHbaseStorageHandlerPrivilegeObject(HivePrivilegeObjectType type, String dbname, String objectName,
      List<String> partKeys, List<String> columns, HivePrivObjectActionType actionType, List<String> commandParams,
      String className, String ownerName, PrincipalType ownerType, Table tableMetadata,
      HiveStorageHandler storageHandler){
    super(type, dbname, objectName, partKeys, columns, actionType, commandParams, className, ownerName, ownerType, tableMetadata, storageHandler);
  }

  @Override
  public void authorizeAction(StoragePrivilege privsRequested) throws HiveException {
    HiveAuthorizationProvider authProvider = storageHandler.getAuthorizationProvider();

    /**
     * TODO: we need to translate StoragePrivileges passed in from Authorizer
     *      into Privilege array that AuthorizationProvider understands. Then we pass
     *     them down to AuthorizationProvider.authorize call.
      */
    if (authProvider != null){
      Privilege[] readPrivileges = new Privilege[1];
      if (privsRequested == StoragePrivilege.CREATE){
        readPrivileges[0] = Privilege.CREATE;
      }
      Privilege[] writePrivileges= null;
      authProvider.authorize(this.tableMetadata, readPrivileges, writePrivileges);
    }
  }
}
