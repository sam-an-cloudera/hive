package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveHbaseStorageHandlerPrivilegeObject;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class HiveHBaseAuthorizationProvider extends HiveAuthorizationProviderBase {
  private Connection hbaseConnection;
  private Admin admin;
  @Override
  public void init(Configuration conf) throws HiveException {
    hive_db = new HiveProxy();
    //TODO: maybe this shoud be HIVE_METASTORE_AUTHENTICATOR_MANAGER? double check later
    authenticator = HiveUtils.getAuthenticator(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER);
    SessionState ss = SessionState.get();
    authenticator.setSessionState(ss);
    try {
      if (admin == null) {
        hbaseConnection = ConnectionFactory.createConnection(conf);
        admin = hbaseConnection.getAdmin();
      }
    } catch (IOException ioe) {
      throw new HiveException(StringUtils.stringifyException(ioe));
    }
  }

  @Override public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

  }

  @Override public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

  }

  @Override public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    String userName = authenticator.getUserName();
    try {
      List<UserPermission> permissionList = AccessControlClient.getUserPermissions(hbaseConnection, userName);
      //TODO: Ramesh had this question: what if Ranger is there serving as authorizer for HBase as well, how does it work here?
      if (readRequiredPriv != null){
        for (Privilege priv: readRequiredPriv){
          if (!hasRightAuthorization(table, priv, permissionList)){
            throw new AuthorizationException("User is not authorized on HBase table");
          }
        }
      }

      if (writeRequiredPriv != null){
        for (Privilege priv: writeRequiredPriv){
          if (!hasRightAuthorization(table, priv, permissionList)){
            throw new AuthorizationException("User is not authorized on HBase table");
          }
        }
      }
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      throw new HiveException("Cannot get user permission from HBase");
    }
  }

  private boolean hasRightAuthorization(Table table, Privilege priv, List<UserPermission> perms){
    TableName hbaseTableName = null; //TODO: translate table name to hbaseTableName
    for (UserPermission perm : perms){
      switch (priv.getPriv()) {
        case CREATE:
        case ALTER_METADATA:
          if (perm.implies(hbaseTableName, null, null, Permission.Action.CREATE)
             || perm.hasTable() && perm.getTableName().equals(table.getTableName())){
              return true;
          }
          break;
        case INSERT:
        case DELETE:
         break;
      default:
        break;
      }
    }
    return false;
  }

  @Override public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

  }

  @Override public void authorize(Table table, Partition part, List<String> columns, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

  }
}
