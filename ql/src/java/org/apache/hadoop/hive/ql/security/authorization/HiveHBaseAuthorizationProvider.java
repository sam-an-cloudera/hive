package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveHbaseStorageHandlerPrivilegeObject;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.security.access.AccessControlClient;

import java.util.List;

public class HiveHBaseAuthorizationProvider extends HiveAuthorizationProviderBase {
  Connection hbaseConnection;
  @Override public void init(Configuration conf) throws HiveException {
    hbaseConnection = null;
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
    //check if the user has read/write access to the table

    try {
      List<UserPermission> permissionList = AccessControlClient.getUserPermissions(hbaseConnection, userName);

    } catch (Throwable throwable) {
      throwable.printStackTrace();
      throw new HiveException("Cannot get user permission from HBase");
    }

  }

  @Override public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

  }

  @Override public void authorize(Table table, Partition part, List<String> columns, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {

  }
}
