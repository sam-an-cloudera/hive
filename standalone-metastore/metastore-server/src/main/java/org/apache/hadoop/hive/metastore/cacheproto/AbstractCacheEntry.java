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

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class of cache entries (in @c CacheMemoryHandler).
 * All cache entries have to be derived from this base class, which
 * helps to create instances based on cacheType (stored with the cache
 * data) and which also provides some general helper functionality to
 * deal with the data, stored in the cache as @c ByteBuffer.
 */
public abstract class AbstractCacheEntry {
    /// maps class names (of derived classes) to a one-byte identifier
    private static ConcurrentHashMap<String,Byte> classToType = new ConcurrentHashMap<>();
    /// maps (back) the one-byte identifier to the class name 
    private static ConcurrentHashMap<Byte,String> typeToClass = new ConcurrentHashMap<>();
    private static int nextValue = 0;  ///< Provides the next classType value

    /// maps outer class names to their objects. This is required if the @c AbstractCacheEntry
    /// implementation is actually an inner class of the outer. We then need to get the reference
    // to the outer class to instantiate new instances of the inner cache entry class.
    private static ConcurrentHashMap<String,Object> outerClassMap = new ConcurrentHashMap<>();

    private   byte classType;      ///< uniquely identify the name of the cache entry class
    protected int  entryID = -1;   ///< the composite (memory block and entry) entry position

    /** 
     * Helper class to deal with variable length data.
     * A class, derived from @c AbstractCacheEntry, has to store its data "flat"
     * within a @c ByteBuffer instance. It is recommended to first store all fixed
     * length fields of the cache entry and then use an instance of this class to
     * store the variable length fields (like String instances) at the end. This
     * class first stores an integer array of length information (for each variable
     * length field) and then the actual data.
     */
    protected static class VarLenPut {
        private ArrayList<byte[]> entries   = new ArrayList<>() ;  ///< array of variable length fields
        private boolean           wasStored = false;               ///< Indicator that store was called

        /**
         * Creates a new instance
         */
        public VarLenPut() {
            // nothing to do here
        }

        /**
         * Adds a byte array as variable length field to the output.
         * 
         * @param data The variable length field
         * @return Reference to self for fluent concat.
         */
        public VarLenPut put( byte[] data ) {    
            checkNotNull( data, "Passed in data can't be null" );
            checkState( !wasStored , "Can't add data after storing" );
            entries.add( data );
            return this;
        }

        /**
         * Adds a String as variable length field to the output.
         * 
         * @param s The variable length string
         * @return Reference to self for fluent concat.
         */
        public VarLenPut put( String s ) {
            checkNotNull( s, "Passed in string can't be null" );
            put( s.getBytes() );
            return this;
        }

        /** 
         * Returns the total size (in bytes) to store all fields.
         * This method can be used in size calculations. The returned
         * size already includes the amount of memory required to store
         * the length information along with the field data.
         * 
         * @return The total size required in bytes
         */
        public int getSize() {
            int ret = 0;

            for ( byte[] b : entries )
                ret += b.length;

            return ret + (Integer.BYTES * entries.size());
        }

        /**
         * Stores the variable length fields in the target buffer.
         * The method first stores all length information as an array of
         * integers and then appends the actual data.
         * 
         * @param target The target byte buffer.
         * @param offset Starting position in @c ByteBuffer
         */
        public void store( ByteBuffer target, int offset ) {
            checkNotNull( target, "The passed in ByteBuffer can't be null" );
            checkArgument( offset < target.limit(), "Offset (%s) too high", offset );
            checkState( !wasStored , "Data was already stored" );
            int bytePos = offset;

            wasStored = true;

            // store the length information first
            for ( byte[] ba : entries ) {
                target.position( bytePos );
                target.putInt( ba.length );
                bytePos += Integer.BYTES;
            }

            // then the data
            for ( byte[] ba : entries ) {
                target.position( bytePos );
                target.put( ba );
                bytePos += ba.length;
            }
        }
    }

    /**
     * Helper to receive variable length fields from a @c ByteBuffer again.
     * An instance of this helper class can be used by a @c AbstractCacheEntry
     * deriving class to read the content of variable length fields again, which
     * were stored via @c VarLenPut before.
     */
    protected static class VarLenGet {
        private ByteBuffer source     = null;  ///< the memory source
        private int[]      lengthData = null;  ///< array of field length information
        private int[]      offsetData = null;  ///< calculated array of field offsets

        /**
         * Creates a new helper to access the content of variable length fields.
         * The caller is pointing this constructor to the start of the variable length
         * section of a cache entry and can then access each field by index.
         * 
         * @param bb The raw memory of the cache entry
         * @param offset Starting position of the entry in memory
         * @param numEntries The known number of entries in the variable length section
         */
        public VarLenGet( ByteBuffer bb, int offset, int numEntries ) {
            checkNotNull( bb, "The passed in ByteBuffer can't be null" );
            checkArgument( offset + numEntries * Integer.BYTES < bb.limit(), 
                           "The offset is too high for that data (beyond limit)" );
            
            source     = bb;
            lengthData = new int[numEntries];
            offsetData = new int[numEntries];

            // read the length information and derive the offsets
            bb.position( offset );
            for ( int i = 0; i < numEntries; ++i ) {
                lengthData[i] = bb.getInt();

                if ( 0 == i ) 
                    offsetData[0] = offset + (numEntries * Integer.BYTES);
                else 
                    offsetData[i] = offsetData[i-1] + lengthData[i-1];
            }
        }

        /**
         * Gives access to a variable length field in bytes.
         * 
         * @param idx Index (zero based) of the entry to access
         * @return A copy of the data of that entry
         */
        public byte[] get( int idx ) {
            checkArgument( 0 <= idx && idx < lengthData.length, "Index out of bounds" );

            byte[] ret = new byte[lengthData[idx]];
            source.position( offsetData[idx] );
            source.get( ret );

            return ret;
        }

        /**
         * Returns the variable length entry as a String.
         * 
         * @param idx The index of the entry to access
         * @return A String, representing the field content
         */
        public String getString( int idx ) {
            return new String( get( idx ) );
        }
    }

    /**
     * Creates a new cache entry, yet withoug values.
     */
    public AbstractCacheEntry() {
        // register this cache entry type and assign unique value to it
        mapCLassType();
    }

    /**
     * Creates a cache entry for a known entry.
     * 
     * @param entryID The composite entry (block and index in block)
     */
    public AbstractCacheEntry( int entryID ) {
        this.entryID = entryID;
        mapCLassType();
    }

    /**
     * Returns the composite entryID.
     * The returned ID is used to reference the cache entry across multiple @c MemoryBlock instances.
     * 
     * @return The cache entry's identifier
     */
    public int getEntryID() {
        return entryID;
    }

    /**
     * Optional registry method to allow cache entries be inner classes.
     * If the cache entry (@c AbstractCacheEntry implementing class) is an inner class of a lerger
     * cache construct, you need to register the outer class (the cache construct) here. This allows
     * creation of instances of the inner class.
     * 
     * Note: Right now, there is only a single instance per cache class allowed!
     * 
     * @param outerClass The outer (cache construct) class instance
     */
    public static void registerCacheEntryContainer( Object outerClass ) {
        checkNotNull( outerClass, "The outer class can't be null" );
        outerClassMap.put( outerClass.getClass().getName(), outerClass );
    }

    /**
     * Removes an outer class reference, added via @c registerCacheEntryContainer.
     * 
     * @param outerClass The reference to the outer class to remove
     */
    public static void unregisterCacheEntryContainer( Object outerClass ) {
        checkNotNull( outerClass, "The outer class can't be null" );
        outerClassMap.remove( outerClass.getClass().getName() );
    }

    /**
     * Factory method to create a cache entry with the right type.
     * This method can be used to produce the right subclass, which is associated
     * with a cache entry. The method is used during evicition of cache entries
     * to create an instance during listener notification.
     * 
     * @param type The (stored) type identifier (like class name entry)
     * @param entryID The unique composite entry of a cache entry
     * @return The concrete subclass of the cache entry for that type
     */
    protected static AbstractCacheEntry create( byte type, int entryID ) {
        String className = typeToClass.get( type );
        if ( null == className ) 
            throw new IllegalArgumentException( "Invalid/unknown type value" );

        try {
            Object instance = null;

            // search either for a constructor that has only one integer (the eventID) or for a
            // constructor that takes an instance of a registered outer class (this of outer) and
            // an integer (the eventID)
            for ( Constructor<?> c : Class.forName( className ).getConstructors() ) {
                if ( 1 == c.getParameterCount() && c.getParameterTypes()[0] == Integer.TYPE ) {
                    instance = c.newInstance( entryID );
                    break;
                }
                else if ( 2 == c.getParameterCount() && c.getParameterTypes()[1] == Integer.TYPE ) {
                    Object outerInstance = outerClassMap.get( c.getParameterTypes()[0].getName() );

                    if ( null != outerInstance ) {
                        instance = c.newInstance( outerInstance, entryID );
                        break;
                    }
                }
            }

            if ( null == instance ) 
                throw new IllegalStateException( "Constructor not found for cache entry implementation" ); 

            if ( instance instanceof AbstractCacheEntry ) {
                return (AbstractCacheEntry)instance;
            }

            throw new IllegalStateException( "The generated class is not an AbstractCacheEntry" );
        }
        catch( ClassNotFoundException cnfe ) {
            throw new IllegalStateException( "The matching AbstractCacheEntry class was not found!" ); 
        }
        catch( Throwable thr ) {
            throw new IllegalStateException( "Unknown error when creationg AbstractCacheEntry:" + thr.getMessage() ); 
        }
    }


    /**
     * The numeric (one-byte) identifier for the class.
     * This value is used to map back the cache entry source type to the class name, as maintained
     * statically by this class. It allows creation of the correct implementation via the factory
     * method @c create.
     * 
     * @return The numeric identifier for the cache entry class
     */
    protected byte getClassType() {
        return classType;
    }

    /**
     * Helper to map the current class to the numeric class type value.
     * Called during instance construction to get the numeric identifier of a class.
     */
    private void mapCLassType() {
        if ( null == classToType.get( getClass().getName() ) ) {
            synchronized( classToType ) {
                if ( null == classToType.get( getClass().getName() ) ) {
                    nextValue += 1;

                    if ( nextValue > Byte.MAX_VALUE ) 
                        throw new IllegalStateException( "Too many different cache entry types" );
    
                    classType = (byte)nextValue;
                    classToType.putIfAbsent( getClass().getName(), classType );
                    typeToClass.putIfAbsent( classType, getClass().getName() );
                }
                else 
                    classType = classToType.get( getClass().getName() );
            }
        }
        else 
            classType = classToType.get( getClass().getName() );
    }
}