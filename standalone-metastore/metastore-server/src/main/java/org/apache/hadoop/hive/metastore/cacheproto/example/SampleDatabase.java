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
 * Example database data container class.
 * This could be any kind of data container, inlcuding the Thrift generated class.
 */
public class SampleDatabase {
    private int databaseID;
    private int catalogID;

    private String name;
    private String description;
    private String location;

    protected SampleDatabase() {
    }

    protected void setCatalogID( int id ) {
        catalogID = id;
    }

    protected void setDatabaseID( int id ) {
        databaseID = id;
    }

    public int getDatabaseID() {
        return databaseID;
    }

    public int getCatalogID() {
        return catalogID;
    }

    public void setName( String n ) {
        name = n;
    }

    public String getName() {
        return name != null?name:"";
    }

    public void setDescription( String d ) {
        description = d;
    } 

    public String getDescription() {
        return description != null?description:"";
    }

    public void setLocation( String l ) {
        location = l;
    }

    public String getLocation() {
        return location != null?location:"";
    }
}