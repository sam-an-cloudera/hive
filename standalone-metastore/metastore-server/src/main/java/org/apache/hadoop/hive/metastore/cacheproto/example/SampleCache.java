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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.cacheproto.AbstractCacheEntry;
import org.apache.hadoop.hive.metastore.cacheproto.CacheMemoryHandler;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Example cache implementation, based on @c EntryEvictionListener.
 * This cache implementation is only an example to show how to use a @c CacheMemoryHandler to
 * store all content of cached items in their flat form within the cache.
 */
public class SampleCache implements Closeable {
    private CacheMemoryHandler memoryHandler = null;  ///< the actual memory for the cache
    
    // these should probably map databaseID and tableID to entryID (instead of name)
    private ConcurrentHashMap<String,Integer> databases = null;  ///< maps database name to the cache entryID
    private ConcurrentHashMap<String,Integer> tables = null;     ///< maps table name to cache entryID

    private CacheMemoryHandler.EntryEvictionListener evictionHandler = null;  ///< handles evictions

    private int numEvictedItems = 0;

    /**
     * Creates a new example cache with 20MB size.
     */
    public SampleCache() {
        memoryHandler = new CacheMemoryHandler( 20 );  // 20MB - this is just a sample
        databases = new ConcurrentHashMap<>();
        tables = new ConcurrentHashMap<>();

        evictionHandler = new EvictionListener();
        memoryHandler.addEntryEvictionListener( evictionHandler );

        // we need to register this outer class because our cache entries are inner classes
        // and AbstractCacheEntry needs to be able to create instances of these.
        AbstractCacheEntry.registerCacheEntryContainer( this );
    }

    /**
     * Closes the sample cache and frees all resources.
     */
    public void close() {
        AbstractCacheEntry.unregisterCacheEntryContainer( this );

        memoryHandler.close();
        databases.clear();
        tables.clear();

        memoryHandler = null;
        databases = null;
        tables = null;
        evictionHandler = null;
    }

    /**
     * Inner helper class to clean up hash maps if an item is evicted from cache.
     * The @c CacheMemoryHandler is calling the @c entryEvicted of this class to inform that an
     * entry was evicted from the LRU cache. The event is used here to remove the entry also from
     * our loopup hash tables.
     */
    private class EvictionListener implements CacheMemoryHandler.EntryEvictionListener {
        @Override
        public void entryEvicted( AbstractCacheEntry ace ) {
            if ( ace instanceof DatabaseCacheEntry ) {
                // also remove the entry from our hashmap, if it got evicted
                databases.remove( ((DatabaseCacheEntry)ace).theDatabase.getName().trim().toLowerCase() );
            }
            else if ( ace instanceof TableCacheEntry ) {
                TableCacheEntry tce      = (TableCacheEntry)ace;
                String          normName = "<" + tce.theTable.getDatabaseName().trim().toLowerCase() +
                                           ">" + tce.theTable.getTableName().trim().toLowerCase();

                tables.remove( normName );                           
            }

            ++numEvictedItems;
        }
    }

    /**
     * Cache entry implementation for @c SampleDatabase objects.
     * This inner class is used to map an POJO @c SampleDatabase obejct into the flat cache
     * representation. It is an adapter between the @c CacheMemoryHandler and the actual cached
     * content.
     */
    private class DatabaseCacheEntry extends AbstractCacheEntry {
        private final static int OFFSET_CATALOG_ID  = 0;
        private final static int OFFSET_DATABASE_ID = OFFSET_CATALOG_ID + Integer.BYTES;
        private final static int OFFSET_VARLEN      = OFFSET_DATABASE_ID + Integer.BYTES;

        private SampleDatabase theDatabase = null;  ///< short term reference to the POJO

        /**
         * Constructor to flatten out the POJO and to store it in the cache.
         * This constructor is used to move the POJO content of the @c SampleDatabase into the
         * cache, actually creating the corresponting cache entryID for it.
         * 
         * @param db The POJO @c SampleDatabase object to store in the cache
         */
        public DatabaseCacheEntry( SampleDatabase db ) {
            VarLenPut vlp = new VarLenPut().put( db.getName() )
                                           .put( db.getDescription() )
                                           .put( db.getLocation() );
            int cacheSize = (Integer.BYTES * 2) + vlp.getSize();

            // store the POJO content to the cache
            memoryHandler.allocateForCacheEntry( this, cacheSize, new CacheMemoryHandler.CacheInitializerFunction(){                
                @Override
                public void fillCache(ByteBuffer bb) {
                    bb.putInt( OFFSET_CATALOG_ID, db.getCatalogID() );
                    bb.putInt( OFFSET_DATABASE_ID, db.getDatabaseID() );
                    vlp.store( bb, OFFSET_VARLEN );
                }
            } );                
        }

        /**
         * Reads the minimum amount of fields from the cache to identify the object.
         * In our case, this is the database name of the POJO @c SampleDatabase. This constructor
         * is used by the @c CacheMemoryHandler if a cache entry is evicted. We use it to identify
         * the database and remove its entry from our lookup hash map. Another use case of this
         * constructor is in conjunction with @c readRemaining to read the whole cache entry.
         * 
         * @param entryID The cache entry identifier
         */
        public DatabaseCacheEntry( int entryID ) {
            super( entryID );

            theDatabase = new SampleDatabase();
            theDatabase.setName( memoryHandler.access( this, new CacheMemoryHandler.CacheAccessFunction<String>() {
                public String access( ByteBuffer bb ) {
                    return (new VarLenGet( bb, OFFSET_VARLEN, 3)).getString( 0 );
                }
            }));
        }

        /**
         * Reads all remaining POJO fields and returns it as @c SampleDatabase.
         * If the instance was created from a cache entry, it initially only read the fields that
         * were required to identify the database object. This method is reading the remaining
         * fields of the @c SampleDatabase POJO.
         * 
         * @return The actual SampleDatabase
         */
        public SampleDatabase readRemaining() {
            return memoryHandler.access( this, new CacheMemoryHandler.CacheAccessFunction<SampleDatabase>() {
                public SampleDatabase access( ByteBuffer bb ) {
                    theDatabase.setCatalogID( bb.getInt( OFFSET_CATALOG_ID ) );
                    theDatabase.setDatabaseID( bb.getInt( OFFSET_DATABASE_ID ) );

                    VarLenGet vlg = new VarLenGet( bb, OFFSET_VARLEN, 3 );
                    theDatabase.setDescription( vlg.getString( 1 ) );
                    theDatabase.setLocation( vlg.getString( 2 ) );
                    return theDatabase;
                }
            });
        }
    }

    /**
     * Cache entry implementation for @c SampleTable objects.
     * This inner class is used to map an POJO @c SampleTable obejct into the flat cache
     * representation. It is an adapter between the @c CacheMemoryHandler and the actual cached
     * content.
     */
    public class TableCacheEntry extends AbstractCacheEntry {
        private final static int OFFSET_DBID      = 0;
        private final static int OFFSET_TBLID     = OFFSET_DBID + Integer.BYTES;
        private final static int OFFSET_WRITERID  = OFFSET_TBLID + Integer.BYTES;
        private final static int OFFSET_RETENTION = OFFSET_WRITERID + Integer.BYTES;
        private final static int OFFSET_VARLEN    = OFFSET_RETENTION + Integer.BYTES;

        private SampleTable theTable = null;  ///< short term reference to the POJO

        /**
         * Constructor to flatten out the POJO and to store it in the cache.
         * This constructor is used to move the POJO content of the @c SampleTable into the
         * cache, actually creating the corresponting cache entryID for it.
         * 
         * @param table The POJO @c SampleTable object to store in the cache
         */
        public TableCacheEntry( SampleTable table ) {
            VarLenPut vlp = new VarLenPut().put( table.getDatabaseName() )
                                           .put( table.getTableName() )
                                           .put( table.getDescription() )
                                           .put( table.getOwner() );
            int cacheSize = (Integer.BYTES * 4) + vlp.getSize();

            memoryHandler.allocateForCacheEntry( this, cacheSize, new CacheMemoryHandler.CacheInitializerFunction(){                
                @Override
                public void fillCache(ByteBuffer bb) {
                    // fixed length fields first
                    bb.putInt( OFFSET_DBID, table.getDatabaseID() );
                    bb.putInt( OFFSET_TBLID, table.getTableID() );
                    bb.putInt( OFFSET_WRITERID, table.getWriterID() );
                    bb.putInt( OFFSET_RETENTION, table.getRetention() );
                    vlp.store( bb, OFFSET_VARLEN );  // then the variable length fields
                }
            } );                
        }

        /**
         * Reads the minimum amount of fields from the cache to identify the object.
         * In our case, this is the database name and the table name of the POJO @c SampleTable. 
         * This constructor is used by the @c CacheMemoryHandler if a cache entry is evicted. We use 
         * it to identify the table and remove its entry from our lookup hash map. Another use case 
         * of this constructor is in conjunction with @c readRemaining to read the whole cache entry.
         * 
         * @param entryID The cache entry identifier
         */
        public TableCacheEntry( int entryID ) {
            super( entryID );

            theTable = new SampleTable();
            memoryHandler.access( this, new CacheMemoryHandler.CacheAccessFunction<Void>() {
                public Void access( ByteBuffer bb ) {
                    VarLenGet vlg = new VarLenGet( bb, OFFSET_VARLEN, 4 );
                    theTable.setDatabaseName( vlg.getString( 0 ));
                    theTable.setTableName( vlg.getString( 1 ) );
                    return null;
                }
            });
        }

        /**
         * Reads all remaining POJO fields and returns it as @c SampleTable.
         * If the instance was created from a cache entry, it initially only read the fields that
         * were required to identify the table object. This method is reading the remaining
         * fields of the @c SampleTable POJO.
         * 
         * @return The actual SampleTable
         */
        public SampleTable readRemaining() {
            return memoryHandler.access( this, new CacheMemoryHandler.CacheAccessFunction<SampleTable>() {
                public SampleTable access( ByteBuffer bb ) {
                    theTable.setDatabaseID( bb.getInt( OFFSET_DBID ) );
                    theTable.setTableID( bb.getInt( OFFSET_TBLID ) );
                    theTable.setWriterID( bb.getInt( OFFSET_WRITERID ) );
                    theTable.setRetention( bb.getInt( OFFSET_RETENTION ) );

                    VarLenGet vlg = new VarLenGet( bb, OFFSET_VARLEN, 4 );
                    theTable.setDescription( vlg.getString( 2 ) );
                    theTable.setOwner( vlg.getString( 3 ) );
                    return theTable;
                }
            });
        }
    }

    /**
     * Returns the cached @c SampleDatabase POJO instance or null if not present.
     * If the @c SampleDatabase was put into the cache before and wasn't evicted (due to cache
     * memory limitations) in the meantine, this method will return an instance of it. It returns
     * null if the object doesn't exist or was evicted.
     * 
     * @param name The database name to look up
     * @return A matching @c SampleDatabase instance or null
     */
    public SampleDatabase getDatabase( String name ) {
        checkNotNull( name, "database name cannot be null" );
        checkArgument( 0 < name.trim().length(), "database name can't be empty string" );

        Integer entryID = databases.get( name.trim().toLowerCase() );
        if ( null != entryID ) 
            return (new DatabaseCacheEntry( entryID ).readRemaining());

        return null;
    }

    /**
     * Stores a new or overwrites an existing @c SampleDatabase instance.
     * This method is used to store the initial version or overwrite an existing version of a POJO
     * @c SampleDatabase object.
     * 
     * @param db The database object to store in the cache
     */
    public void setDatabase( SampleDatabase db ) {
        checkNotNull( db, "Passed in database is null" );

        String  normName = db.getName().trim().toLowerCase();
        Integer entryID  = databases.get( normName );

        if ( null != entryID ) 
            memoryHandler.discard( entryID );

        // DatabaseCacheEntry constructor stores the entry and delivers cache entry ID
        databases.put( normName, (new DatabaseCacheEntry( db )).getEntryID() );
    }

    /**
     * Returns the cached @c SampleTable POJO instance or null if not present.
     * If the @c SampleTable was put into the cache before and wasn't evicted (due to cache
     * memory limitations) in the meantine, this method will return an instance of it. It returns
     * null if the object doesn't exist or was evicted.
     * 
     * @param databaseName The database name to look up
     * @param tableName The name of the table within that database
     * @return A matching @c SampleTable instance or null
     */
    public SampleTable getTable( String databaseName, String tableName ) {
        String  normName = "<" + databaseName.trim().toLowerCase() + ">" + tableName.trim().toLowerCase();
        Integer entryID  = tables.get( normName );

        if ( null != entryID )
            return (new TableCacheEntry( entryID )).readRemaining();

        return null;
    }

    /**
     * Stores a new or overwrites an existing @c SampleTable instance.
     * This method is used to store the initial version or overwrite an existing version of a POJO
     * @c SampleTable object.
     * 
     * @param table The @c SampleTable to store in the cache
     */
    public void setTable( SampleTable table ) {
        String  normName = "<" + table.getDatabaseName().trim().toLowerCase() +
                           ">" + table.getTableName().trim().toLowerCase();
        Integer entryID  = tables.get( normName );

        if ( null != entryID ) 
            memoryHandler.discard( entryID );

        // TableCacheEntry constructor stores the entry and delivers cache entry ID
        tables.put( normName, (new TableCacheEntry( table )).getEntryID() );    
    }

    /**
     * Returns the total number of evicted items.
     * This method is here mainly for testing purposes. It returns the amount of evict calls that
     * we registered.
     * 
     * @return The total amount of cache evictions
     */
    @VisibleForTesting
    public int getNumEvicted() {
        return numEvictedItems;
    }
}