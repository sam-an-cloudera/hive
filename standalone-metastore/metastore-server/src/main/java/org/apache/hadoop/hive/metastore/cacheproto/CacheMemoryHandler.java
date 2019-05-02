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
package org.apache.hadoop.hive.metastore.cacheproto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;

import java.io.Closeable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates a LRU based cache structure, storing @c AbstractCacheEntry instances.
 * This class uses multiple @c MemoryBlock instances to manage memory off heap, using
 * it to store instances of type @c AbstarctCacheEntry. This class takes care of 
 * storing the double linked list together with the cache entries to implement the
 * LRU and to maintain the linked list upon access of elements. It is also finding
 * the memory on its @c MemoryBlock instances or causing eviction of entries if the
 * maximum size is reached.
 * This class is not a cache implementation by itself. It delivers the infrastructure
 * to create a cache with LRU and fixed sized (off heap) memory. It defines an 
 * @c EntryEvictionListener interface, which should be implemented by the actual
 * cache implementation. It is used to notify about entries that are evicted from
 * the cache.
 * Cache content (entries) is represented by instances of classes which are derived
 * from the @c AbstractCacheEntry class. These instances are responsible for reading
 * or writing their entry content into a @c ByteBuffer. 
 * 
 * <b>Note on the cache memory:</b>
 * We use 4MB @c MemoryBlock instances in the backend. So, we will round up the 
 * amount of (through constructor) configured memory to the next multiple of 4MB. As
 * we only have 15 bits to handle the block index within a composite entryID, we have
 * a maximum size of 128GB possible for a single @c CacheMemoryHandler.
 */
public class CacheMemoryHandler implements Closeable {
    private final static int PREVIOUS = 0;                     ///< offset of "previous" index
    private final static int NEXT     = Integer.BYTES;         ///< offset of "next" index
    private final static int TYPE     = NEXT + Integer.BYTES;  ///< offset of class type index
    private final static int NONE     = -1;                    ///< used for readability

    private ArrayList<EntryEvictionListener> listeners = null; ///< all eviction listeners
    private ArrayList<MemoryWrapper> memoryBlocks = null;      ///< the memory blocks of this handler
    private AtomicReference<MemoryWrapper> currentMemoryBlock = null; ///< currently used memory block
    private AtomicReference<MemoryWrapper> nextBestBlock = null;      ///< next block, determinded by memory optimizer
    private final int maxBlocks;                               ///< max. number of 4MB blocks
    private int headOfChain;                                   ///< head of LRU chain (entryID)
    private int tailOfChain;                                   ///< tail of LRU chain (entryID)
    private ScheduledExecutorService memoryOptimizer = null;   ///< background memory optimization task

    /**
     * Function to initialize a new cache element.
     * A class, derived from @c AbstractCacheEntry, should implement and use
     * this function (and pass it in to @c allocateForCacheEntry) to populate
     * the cache entry's memory with the data of the entry. This could for 
     * example be serializing cache class fields into the provided byte 
     * buffer. Incapsulating the @c ByteBuffer initialization into an own
     * function, ensures serialization on the underlying @c MemoryBlock.
     */
    public static interface CacheInitializerFunction {
        /**
         * Called to populate/initialize the memory content of a new cache
         * entry. This function is called from within the @c allocateForCacheEntry
         * method to write the content into a new cache entry while being 
         * propperly synchronized.
         * 
         * @param bb The ByteBuffer to fill with the new entry's content
         */
        public void fillCache( ByteBuffer bb );
    }

    /**
     * Function to access the content of cache entry.
     * A @c AbstractCacheEntry derived class should use this function to
     * gain access to the memory of the cached entry. The @c CacheMemoryHandler
     * will ensure synchronization around the cached entry, esuring that the
     * memory remains stable while being accessed.
     */
    public static interface CacheAccessFunction<T> {
        /**
         * Called by the @c CacheMemoryHandler to give access to cache data.
         * The cache entry implementing class should implement this function
         * to read or write specfic content for the cache entry memory.
         * 
         * @param bb ByteBuffer with the entry's memory
         * @return Optional return value of the access function
         */
        public T access( ByteBuffer bb );
    }

    /**
     * Event listener interface for evicition notifications.
     * The event listeners, registered to the @c CacheMemoryHandler,
     * are called before the object is actually evicted. The class, 
     * which implements the actual cache around this @c CacheMemoryHandler
     * should implement the listener to be triggered on LRU based evicitions,
     * allowing it to perform additional cleanup (removing references to the
     * evicted entry).
     */
    public static interface EntryEvictionListener {
        /**
         * Called whenever a @c AbstractCacheEntry is evicted.
         * The @c CacheMemoruHandler actually instantiates an instance of 
         * the @c AbstractCacheEntry deriving class (matching that particular
         * cache entry), passing it in to the event listener. The listener
         * can use the passed in instance to figure out what was evicted to
         * remove all references (outside of the cache) to it.
         * 
         * @param ace The evicted cache entry
         */
        public void entryEvicted( AbstractCacheEntry ace );
    }

    /**
     * Helper wrapper around @c MemoryBlock.
     * Extends the @c MemoryBlock by an index information that is then
     * used as part of the entryID.
     */
    private class MemoryWrapper extends MemoryBlock {
        public int blockIndex;   ///< the index into memoryBlocks

        /**
         * Crates a new instance and wraps the new index.
         * 
         * @param idx The memoryBlocks index of this instance
         */
        public MemoryWrapper( int idx ) {
            // we use a block size of 4MB (off-heap memory)
            super( 4 * 1024 * 1024 , true );
            blockIndex = idx;
        }
    }

    /**
     * Helper class to access the double linked list values of an entry.
     * Every cached entry (instance derived from @c AbstractCacheEntry) is stored
     * flat into memory of the cache. Each entry in the cache is prefixed by a
     * "previous", "next" and "type" field. As each cache entry is indeitfied by
     * an integer, the "previous" and "next" are both integers pointing to the
     * neighboring elements in the double linked list. The "type" field is a 
     * single byte, defining which class was used to create a cache entry. So,
     * each actualy cache entry is prefixed by 9 (2 integers, 1 byte) bytes,
     * used by this @c CacheMemoryHandler to maintain the double linked list.
     * This class gives access to these fields, based on entries identifier.
     */
    private class CacheEntryPrefix {
        private MemoryWrapper memBlock; ///< reference to memory block of entry
        private int         entryID;    ///< original (composite) entryID
        private int         blockIdx;   ///< block portion of entryID
        private int         inBlockIdx; ///< index within memBlock of entryID
        private int         previous;   ///< "pointer" to previous (younger) LRU chain element
        private int         next;       ///< "pointer" to next (older) LRU chain element
        private byte        classType;  ///< identifier for the class, mapped to the entry

        /**
         * Creates a new helper instance to access the prefix field information.
         * The constructor finds the @c MemoryBlock for the given entryID and then
         * receives the cache entry data to extract the prefix fields into local
         * variables for easier, direct access.
         * 
         * @param entryID The unique (composite) cache entry identifier
         */
        public CacheEntryPrefix( int entryID ) {
            this.entryID = entryID;

            // the upper 16 bits of the entryID sprecify the MemoryBlock index
            // the lower 16 bits of the entryID provide the entry within that MemoryBlock
            blockIdx = (entryID & 0x7FFF0000) >> 16;            
            inBlockIdx = entryID & 0xFFFF;

            synchronized( CacheMemoryHandler.this ) {
                synchronized( memoryBlocks ) {
                    assert( blockIdx >= 0 && blockIdx < memoryBlocks.size() );
                    memBlock = memoryBlocks.get( blockIdx );
                }
            }

            // extract prefix fields, synchronized on memory block
            synchronized( memBlock ) {
                ByteBuffer bb = memBlock.getBufferForIndex( inBlockIdx );
                previous = bb.getInt( PREVIOUS );
                next = bb.getInt( NEXT );
                classType = bb.get( TYPE );
            }
        }

        /**
         * Updates the "previous" entryID of the represented cache entry.
         * This method doesn't only update the local instance variable but also
         * the cache entry's memory in the @c MemoryBlock.
         * 
         * @param newPrev The new value for "previous"
         */
        public void setPrevious( int newPrev ) {
            synchronized( memBlock ) {
                ByteBuffer bb = memBlock.getBufferForIndex( inBlockIdx );
                bb.putInt( PREVIOUS, newPrev );
                previous = newPrev;
            }
        }

        /**
         * Updates the "next" entryID of the represented cache entry.
         * This method doesn't only update the local instance variable but also
         * the cache entry's memory in the @c MemoryBlock.
         * 
         * @param newPrev The new value for "next"
         */
        public void setNext( int newNext ) {
            synchronized( memBlock ) {
                ByteBuffer bb = memBlock.getBufferForIndex( inBlockIdx );
                bb.putInt( NEXT, newNext );
                next = newNext;
            }
        }

        @Override 
        public String toString() {
            StringBuffer sb = new StringBuffer();

            sb.append( "{ \"entryID\": " );
            sb.append( entryID );
            sb.append( ", \"previous\": " );
            sb.append( previous );
            sb.append( ", \"next\": " );
            sb.append( next );
            sb.append( ", \"type\": " );
            sb.append( (int)classType );
            sb.append( " }" );

            return sb.toString();
        }
    }

    /**
     * Creates a new LRU based memory handler, usable by cache implementations.
     * The so created instance initially has only one @c MemoryBlock reserved but
     * can grow up to the specified amount of memory blocks. If all blocks are full
     * and a new entry needs to be added, the oldest not used entry will be evicted
     * to make room for the new entries.
     * 
     * @param maxSizeInMB The total cache memory size (off heap) in MB
     */
    public CacheMemoryHandler( int maxSizeInMB ) {
        checkArgument( maxSizeInMB > 0, "The specified size has to be greater than zero." );
        checkArgument( maxSizeInMB <= (0x7FFF * 4), "The maximum cache size is exceeded" );

        // get the first memory block (each sized 4MB)
        MemoryWrapper mw = new MemoryWrapper( 0 );

        if ( 0 != maxSizeInMB % 4 )
            maxBlocks = (maxSizeInMB / 4) + 1;  // round up to the next 4MB step
        else
            maxBlocks = maxSizeInMB / 4;        // was a multiple of 4MB

        memoryBlocks = new ArrayList<>();
        currentMemoryBlock = new AtomicReference<>( mw );
        nextBestBlock = new AtomicReference<>( null );
        memoryBlocks.add( mw );

        listeners = new ArrayList<>();
        headOfChain = NONE;
        tailOfChain = NONE;

        // create an executor to run our memory optimization all 60 seconds
        memoryOptimizer = Executors.newScheduledThreadPool( 1, new ThreadFactory() {
            @Override
            public Thread newThread( Runnable r ) {
                Thread t = Executors.defaultThreadFactory().newThread( r );
                t.setName("CacheMemoryHandler - MemoryOptimizer" );
                t.setDaemon( true );
                return t;
            }
        } );

        memoryOptimizer.scheduleAtFixedRate( new Runnable() {
            public void run() {
                optimizeMemory();
            }
        }, 120, 60, TimeUnit.SECONDS );
    }

    /**
     * Dereferences all resources
     */
    public void close() {
        memoryOptimizer.shutdown();
        memoryOptimizer = null;

        synchronized( memoryBlocks ) {
            for ( MemoryWrapper mw : memoryBlocks ) {
                mw.close();
            }    

            memoryBlocks.clear();
            memoryBlocks = null;
        }

        currentMemoryBlock = null;

        listeners.clear();
        listeners = null;

        headOfChain = NONE;
        tailOfChain = NONE;
    }

    /**
     * Allocates the memory for a given cache entry.
     * The passed in @c AbstractCacheEntry must not have any memory assigned to
     * it yet. The method then allocates the specified amount of memory in the
     * off heap memory and stores the unique entry identifier in the cache entry.
     * 
     * @param ce The cache entry which needs memory assigned
     * @param size The amount of required memory in bytes
     */
    public void allocateForCacheEntry( AbstractCacheEntry ce, int size) {
        allocateForCacheEntry( ce, size, null );
    }

    /**
     * Allocates the memory for a given cache entry and performs its initialization.
     * The passed in @c AbstractCacheEntry must not have any memory assigned to it yet. 
     * The method then allocates the specified amount of memory in the off heap memory 
     * and stores the unique entry identifier in the cache entry. After that, the passed
     * int initializer function is called to store/initialize the cache memory. 
     * 
     * @param ce The cache entry which needs memory assigned
     * @param size The amount of required memory in bytes
     * @param init The initializer to call to set/populate the new memory
     */
    public void allocateForCacheEntry( AbstractCacheEntry ce, int size, CacheInitializerFunction init ) {
        checkNotNull( ce, "The AbstractCacheEntry can't be null" );
        checkState( NONE == ce.entryID, "The AbstractCacheEntry is already allocated" );

        // reserve 9 bytes more than requested for "previous", "next" and "type"
        ce.entryID = allocate( size + (Integer.BYTES * 2) + Byte.BYTES );
        assert( ce.entryID != NONE );

        // get the memory block from which we just allocated
        int block = (ce.entryID & 0x7FFF0000) >> 16;
        int entry = ce.entryID & 0xFFFF;

        MemoryWrapper mw = null;
        synchronized( this ) {
            synchronized( memoryBlocks ) {
                mw = memoryBlocks.get( block );
                assert( null != mw );
            }

            synchronized( mw ) {
                ByteBuffer bb = mw.getBufferForIndex( entry );

                bb.putInt( PREVIOUS, NONE );       // set the previous of this to non - existing
                bb.putInt( NEXT, headOfChain );    // old head of chain is now 2nd in line
                bb.put( TYPE, ce.getClassType() ); // class tyoe identifier

                // let the previous chain head element point to this new entry
                if ( NONE != headOfChain ) 
                    (new CacheEntryPrefix( headOfChain )).setPrevious( ce.getEntryID() );

                if ( null != init ) {
                    // provide a sub - slice of the buffer to the initializer
                    bb.position( (Integer.BYTES * 2) + Byte.BYTES );
                    init.fillCache( bb.slice() );
                }
            }

            // this is the new head of the chain
            headOfChain = ce.entryID;
            if ( NONE == tailOfChain )
                tailOfChain = ce.entryID;
        }
    }

    /**
     * Gives access to the raw memory of a cache entry.
     * The @c AbstractCacheEntry derived class can use this method in accessor methods
     * to get access to the cache entry's raw memory. The access is provided through
     * a @c CacheAccessFunction to allow synchronized access to the memory.
     * 
     * @param ce The cache entry for which to get the raw cache memory
     * @param caf The function that is called from within @c access to provide the data
     * @return Whatever the @c CacheAccessFunction is returning
     */
    public <T> T access( AbstractCacheEntry ce, CacheAccessFunction<T> caf ) {
        checkNotNull( ce, "The AbstractCacheEntry can't be null" );
        checkState( ce.entryID != NONE, "The AbstractCacheEntry is not having memory yet" );

        CacheEntryPrefix ci = new CacheEntryPrefix( ce.entryID );
        assert( ci.classType == ce.getClassType() );

        // synchronize on this instance while changing head/tail of the LRU chain
        synchronized( this ) {
            if ( ci.entryID != headOfChain ) {
                // unchain the entry
                CacheEntryPrefix prev = new CacheEntryPrefix( ci.previous );
                prev.setNext( ci.next );

                if ( tailOfChain == ci.entryID ) 
                    tailOfChain = prev.entryID;

                if ( NONE != ci.next ) 
                    (new CacheEntryPrefix( ci.next )).setPrevious(( ci.previous ));

                // make this the new head of the LRU chain
                ci.setPrevious( NONE );
                ci.setNext( headOfChain );

                // update the head of the chain
                (new CacheEntryPrefix( headOfChain )).setPrevious( ci.entryID );
                headOfChain = ci.entryID;
            }
        }

        synchronized(ci.memBlock) {
            ByteBuffer bb = ci.memBlock.getBufferForIndex( ci.inBlockIdx );

            // skip the "previous", "next" and "classType" prefix
            bb.position( bb.position() + (Integer.BYTES * 2) + Byte.BYTES );  
            return caf.access( bb.slice() );
        }
    }

    /**
     * Removes an entry from the cache memory.
     * This method can be used to remove an entry from the cache and free
     * the associated memory. 
     * 
     * @param ce The cache entry to remove
     */
    public void discard( AbstractCacheEntry ce ) {
        checkNotNull( ce, "The AbstractCacheEntry can't be null" );
        discard( ce.entryID );
        ce.entryID = NONE;
    }

    /**
     * Removes an entry from the cache memory.
     * This method can be used to remove an entry from the cache and free
     * the associated memory. 
     * 
     * @param entryID The composite entryID to discard
     */
    public void discard( int entryID ) {
        checkState( NONE == entryID, "The AbstractCacheEntry is already deallocated" );

        // unchain this entry
        CacheEntryPrefix ci = new CacheEntryPrefix( entryID );

        if ( NONE != ci.previous ) 
            (new CacheEntryPrefix( ci.previous )).setNext( ci.next );

        if ( NONE != ci.next ) 
            (new CacheEntryPrefix( ci.next )).setPrevious( ci.previous );

        synchronized( this ) {
            if (ci.entryID == headOfChain) 
                headOfChain = ci.next;

            if ( ci.entryID == tailOfChain ) 
                tailOfChain = ci.previous;
        }

        // finally release the cache memory
        ci.memBlock.freeEntry( ci.inBlockIdx );
    }

    /**
     * Adds a listener for cache eviction notifications.
     * The @c CacheMemoryHandler is notifying all listeners before the cache
     * entry is actually evicted, giving them a change for cleaning up and
     * removing references to the cache entry. This method allows adding
     * another listener to the memory handler.
     * 
     * @param eel The listener to add
     */
    public void addEntryEvictionListener( EntryEvictionListener eel ) {
        checkNotNull( eel, "Passed in listener cannot be null" );
        synchronized( listeners ) {
            if ( !listeners.contains( eel ))
                listeners.add( eel );
        }
    }

    /**
     * Removes a previously added eviction notification listener.
     * 
     * @param eel The listener to remove.
     */
    public void removeEntryEvictionListener( EntryEvictionListener eel ) {
        checkNotNull( eel, "Passed in listener cannot be null" );
        synchronized( listeners ) {
            listeners.remove( eel );
        }
    }

    /**
     * Helper to allocate the requested amount of cache memory.
     * This method is used by the public allocate methods to get the
     * raw memory for a cache entry (and the LRU prefix). It goes through
     * the @c MemoryBlock instances to find some available memory there.
     * 
     * @param size The amount of required memory in bytes
     * @return The composite entryID (block and index in block)
     */
    private synchronized int allocate( int size ) {
        int newEntry = NONE;

        // ensure that we have at least 47 bytes per entry. This makes sure that we never have
        // more entries in a 4MB MemoryBlack than what we can index with 16bit. An entry actually
        // needs to have 64 bytes for 2^16 entries to fill up 4MB. But each entry takes 8 bytes
        // for the slot and length information (each one integer) within the @c MemoryBlock
        // leaving 56 bytes for our payload.
        size = max( size, 56 );  

        // loop until memory was found
        while( true ) {
            newEntry = currentMemoryBlock.get().newEntry( size );

            // current block can't satisfy the request?
            if ( NONE == newEntry ) {
                // maybe the memory optimizer already found the next block with most available space?
                MemoryWrapper findNext = nextBestBlock.getAndSet( null );

                // no, so we need to seargc for it by ourself
                if ( null == findNext ) {
                    // find the block with enough available memory
                    synchronized( memoryBlocks ) {
                        for ( MemoryWrapper mw : memoryBlocks ) {
                            if ( size <= mw.getAvailable() ) {
                                findNext = mw;
                                break;
                            }
                        }
                    }
                }

                // we didn't find any block that could satisfy the request?
                if ( null == findNext ) {
                    // can we grow a new memory block?
                    boolean couldGrow = false;

                    synchronized( memoryBlocks ) {
                        if ( memoryBlocks.size() < maxBlocks ) {
                            MemoryWrapper mw = new MemoryWrapper( memoryBlocks.size() );
                            currentMemoryBlock.set( mw );
                            memoryBlocks.add( mw );
                            couldGrow = true;
                        }
                    }

                    if ( !couldGrow ) {
                        // we're full and need to evict entries
                        evict( size );
                    }
                }
                else {
                    // found a block with available memory
                    currentMemoryBlock.set( findNext );
                }
            }
            else 
                break;  // memory was found
        }

        // create composite of block index and entry within that block
        return currentMemoryBlock.get().blockIndex << 16 | (newEntry & 0xFFFF);
    }

    /**
     * Evicts cache entries until the specified amount of memory is available.
     * This helper method is called if we don't find any memory on any other
     * @c MemoryBlock to satisfy an allocation request. This method simply 
     * chops of the tail of the LRU chain and evicts the entry until we have a
     * @c MemoryBlock that has enough space available.
     * 
     * @param size The amount of bytes that is required to be available
     */
    private void evict( int size ) {
        while( true ) {
            CacheEntryPrefix ci = new CacheEntryPrefix( tailOfChain );

            // remove tail from chain
            if ( NONE != ci.previous ) {
                CacheEntryPrefix prev = new CacheEntryPrefix(( ci.previous ) );
                prev.setNext( NONE );
                tailOfChain = prev.entryID;
            }

            // notify about eviction
            synchronized( listeners ) {
                for ( EntryEvictionListener eel : listeners ) 
                    eel.entryEvicted( AbstractCacheEntry.create( ci.classType, ci.entryID ) );
            }

            // free the memory of the last entry
            ci.memBlock.freeEntry( ci.inBlockIdx );
            if ( size <= ci.memBlock.getAvailable() ) {
                // stop here, if we now have enough memory
                currentMemoryBlock.set( ci.memBlock );
                break;
            }
        }
    }

    /**
     * Helper to perform some background optimization.
     * This method is called once per minute to perform some background memory
     * optimization. These optimizations are not really required for functionality
     * but potentially reduce latency upon allocations. It finds the next best 
     * memory block (with most available space) and compacts @c MemoryBlock instances
     * if they have more than 10% reusable space. Both operations would normally
     * happen synchronously when required, potentially slowing down the allocation.
     */
    private void optimizeMemory() {
        // find the block with most available space (as next currentBlock)
        if ( null == nextBestBlock.get() ) {
            ArrayList<MemoryWrapper> toCompact = new ArrayList<>();
            MemoryWrapper blockWithMostSpace = null;
            int nextAvailable = -1;

            synchronized( memoryBlocks ) {
                for ( MemoryWrapper mw : memoryBlocks ) {
                    if ( mw != currentMemoryBlock.get() ) {
                        // find the block with most abailable space
                        if ( nextAvailable < mw.getAvailable() ) {
                            nextAvailable = mw.getAvailable();
                            blockWithMostSpace = mw;
                        }
    
                        // compact the memory block if more than 10% are in reusable
                        if ( 0.1 <= ((float)mw.getReusableSlotCount() / (float)mw.getSlotCount()) ) 
                            toCompact.add( mw );
                    }
                }
            }

            // perform compaction outside of memoryBlock synchronization
            for ( MemoryWrapper mw : toCompact ) 
                mw.compact();

            nextBestBlock.compareAndSet( null, blockWithMostSpace );
        }
    }
}