package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveHbaseStorageHandlerPrivilegeObject;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class HiveHBaseAuthorizationProvider extends HiveAuthorizationProviderBase {
  private Connection hbaseConnection;
  private Admin admin;
  @Override
  public void init(Configuration conf) throws HiveException {
    hive_db = new HiveProxy();
    setConf(conf);
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
    /**
     * TODO: Use HBase AccessControlClient to get the user permissions and compare them against the required RW
     * privileges.
     */
    try {
      List<UserPermission> permissionList = AccessControlClient.getUserPermissions(hbaseConnection, userName);
      for (UserPermission perm : permissionList){
        //if (perm.implies(table.getTableName(),  )
        if (perm.hasTable()){
          if (perm.getTableName().equals(table.getTableName())){
            Permission.Action[] actions = perm.getActions();
            for (Permission.Action action: actions){
              if(action.equals(Permission.Action.CREATE)){
                return;
              }
            }
          }
        }
      }
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      throw new HiveException("Cannot get user permission from HBase");
    }

    throw new AuthorizationException("user is not authorized");

  }

  @Override public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

  }

  @Override public void authorize(Table table, Partition part, List<String> columns, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

  }
}
