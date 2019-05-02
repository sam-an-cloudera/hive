/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.metastore.cacheproto.example;

/**
 * Example table data container class.
 * This could be any kind of data container, inlcuding the Thrift generated class.
 */
public class SampleTable {
    private int databaseID;
    private int tableID;
    private String databaseName;
    private String tableName;
    private String description;
    private String owner;
    private int writerID;
    private int retention;

    protected SampleTable() {
        // nothing to do        
    }

    protected void setDatabaseID( int id ) {
        databaseID = id;
    }

    public int getDatabaseID() {
        return databaseID;
    }

    protected void setTableID( int id ) {
        tableID = id;
    }

    public int getTableID() {
        return tableID;
    }

    public void setDatabaseName( String dbn ) {
        databaseName = dbn;
    }

    public String getDatabaseName() {
        return databaseName!=null?databaseName:"";
    }

    public void setTableName( String tn ) {
        tableName = tn;
    }

    public String getTableName() {
        return tableName != null?tableName:"";
    }

    public void setDescription( String d ) {
        description = d;
    } 

    public String getDescription() {
        return description != null?description:"";
    }

    public void setOwner( String o ) {
        owner = o;
    }

    public String getOwner() {
        return owner != null?owner:"";
    }

    public void setWriterID( int id ) {
        writerID = id;
    }

    public int getWriterID() {
        return writerID;
    }

    public void setRetention( int r ) {
        retention = r;
    }

    public int getRetention() {
        return retention;
    }
}