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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSampleCache {
    private SampleCache cache = null;

    @Before
    public void setUp() {
        cache = new SampleCache();
    }

    @After
    public void tearDown() {
        cache.close();
        cache = null;
    }

    @Test
    public void testSimpleCacheUse() {
        String[] databaseNames = { "MyDatabase", "SampleDB", "MainDB" };
        String[] tableNames    = { "OlliTab", "Customers", "Products", "Orders", "LineItems" };
        int      ident         = 0;

        for ( int i = 0; i < databaseNames.length; ++i ) {
            SampleDatabase newDb = new SampleDatabase();
            newDb.setCatalogID( 17 );
            newDb.setDatabaseID( ident++ );
            newDb.setName( databaseNames[ i ] );
            newDb.setDescription( "Description of " + databaseNames[i] );
            newDb.setLocation( "Location of " + databaseNames[i] );

            cache.setDatabase( newDb );

            for ( int j = 0; j < tableNames.length; ++j ) {
                SampleTable tab = new SampleTable();
                tab.setDatabaseID( newDb.getDatabaseID() );
                tab.setDatabaseName( newDb.getName() );
                tab.setTableName( tableNames[j] );
                tab.setDescription( "Description of table " + tableNames[j] );
                tab.setOwner( "Olli" );
                tab.setRetention( 42 );
                tab.setTableID( ident++ );
                tab.setWriterID( 21 );

                cache.setTable( tab );
            }
        }

        ident = 0;
        for ( int i = 0; i < databaseNames.length; ++i ) {
            SampleDatabase db = cache.getDatabase( databaseNames[i] );
            
            assertNotNull(db);
            assertEquals( 17, db.getCatalogID() );
            assertEquals( ident++, db.getDatabaseID() );
            assertEquals( databaseNames[i], db.getName() );
            assertEquals( "Description of " + databaseNames[i], db.getDescription() );
            assertEquals( "Location of " + databaseNames[i], db.getLocation() );

            for ( int j = 0; j < tableNames.length; ++j ) {
                SampleTable tab = cache.getTable( db.getName(), tableNames[j] );

                assertNotNull( tab );
                assertEquals( db.getDatabaseID(), tab.getDatabaseID() );
                assertEquals( db.getName(), tab.getDatabaseName() );
                assertEquals( tableNames[j], tab.getTableName() );
                assertEquals( "Description of table " + tableNames[j], tab.getDescription() );
                assertEquals( "Olli", tab.getOwner() );
                assertEquals( 42, tab.getRetention() );
                assertEquals( ident++, tab.getTableID() );
                assertEquals( 21, tab.getWriterID() );
            }
        }
    }

    @Test
    public void testEviction() {
        int numTables = 0;

        while( 0 == cache.getNumEvicted() ) {
            SampleTable tab = new SampleTable();
            tab.setDatabaseID( 42 );
            tab.setDatabaseName( "dummyDB" );
            tab.setTableName( "TestTab-" + numTables );
            tab.setDescription( "This is just a dummy table" );
            tab.setOwner( "Olli" );
            tab.setRetention( 42 );
            tab.setTableID( numTables );
            tab.setWriterID( 21 );
            cache.setTable( tab );

            ++numTables;
        }

        // ensure that the first/oldest table got evicted
        assertEquals( 1, cache.getNumEvicted() );
        assertNull( cache.getTable( "dummyDB", "TestTab-" + 0 ) );
        assertNotNull( cache.getTable( "dummyDB", "TestTab-" + 1 ) );
    }
}
