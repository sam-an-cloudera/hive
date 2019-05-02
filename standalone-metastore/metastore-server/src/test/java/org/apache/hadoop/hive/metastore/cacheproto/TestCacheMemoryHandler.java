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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.Test;

public class TestCacheMemoryHandler {
    private ArrayList<CacheMemoryHandler> handlersToClose = new ArrayList<>();

    @After
    public void cleanUp() {
        for ( CacheMemoryHandler cmh : handlersToClose ) 
            cmh.close();

        handlersToClose.clear();
    }

    private static class DummyCacheEntry extends AbstractCacheEntry {
        private final static int OFFSET_SOMEINT    = 0;
        private final static int OFFSET_SOMELONG   = OFFSET_SOMEINT + Integer.BYTES;
        private final static int OFFSET_SOMESTRING = OFFSET_SOMELONG + Long.BYTES;

        public DummyCacheEntry( CacheMemoryHandler cmh, String someString, int someInt, long someLong ) {
            VarLenPut vlp       = (new VarLenPut()).put( someString);
            int       totalSize = vlp.getSize() + Integer.BYTES + Long.BYTES;

            cmh.allocateForCacheEntry( this, totalSize, new CacheMemoryHandler.CacheInitializerFunction(){
                @Override
                public void fillCache(ByteBuffer bb) {
                    bb.putInt( OFFSET_SOMEINT, someInt );
                    bb.putLong( OFFSET_SOMELONG , someLong );
                    vlp.store( bb, OFFSET_SOMESTRING );
                }
            } );
        }

        public DummyCacheEntry( int entryID ) {
            super( entryID );
        }

        public int getSomeInt( CacheMemoryHandler cmh ) {
            return cmh.access( this, new CacheMemoryHandler.CacheAccessFunction<Integer>(){
                @Override
                public Integer access(ByteBuffer bb) {
                    return bb.getInt( OFFSET_SOMEINT );
                }   
            });
        }

        public long getSomeLong( CacheMemoryHandler cmh ) {
            return cmh.access( this, new CacheMemoryHandler.CacheAccessFunction<Long>(){
                @Override
                public Long access(ByteBuffer bb) {
                    return bb.getLong( OFFSET_SOMELONG );
                }
            });
        }

        public String getSomeString( CacheMemoryHandler cmh ) {
            return cmh.access( this, new CacheMemoryHandler.CacheAccessFunction<String>(){
                @Override
                public String access(ByteBuffer bb) {
                    return (new VarLenGet( bb, OFFSET_SOMESTRING, 1 ) ).getString( 0 );
                }
            });
        } 
    }

    private static class TestListener implements CacheMemoryHandler.EntryEvictionListener {
        private ArrayList<HashMap<String,Object>> evictedEntries = new ArrayList<>();
        private CacheMemoryHandler cmh;

        public TestListener( CacheMemoryHandler cmh ) {
            this.cmh = cmh;
        }

        @Override
        public void entryEvicted(AbstractCacheEntry ace) {
            HashMap<String,Object> map = new HashMap<>();
            map.put( "entryID", ace.getEntryID() );

            if ( ace instanceof DummyCacheEntry ) {
                DummyCacheEntry dce = (DummyCacheEntry)ace;
                map.put( "long", dce.getSomeLong( cmh ) );
                map.put( "int", dce.getSomeInt( cmh ) );
                map.put( "string", dce.getSomeString( cmh ) );
            }

            evictedEntries.add( map );
        }

        public List<HashMap<String,Object>> getEvicted() {
            return evictedEntries;
        }
    }

    @Test
    public void testCreateAcrossBlocks() {
        final String myString = "Here comes the rain";

        CacheMemoryHandler cmh         = new CacheMemoryHandler( 8 );  // 8MB
        ArrayList<Integer> entryIDList = new ArrayList<>();
        int                i           = 0;

        handlersToClose.add( cmh );

        while( true ) {
            DummyCacheEntry dce = new DummyCacheEntry( cmh, myString, i, i );

            assertEquals( myString, dce.getSomeString( cmh ) );
            assertEquals( i, dce.getSomeLong( cmh ) );
            assertEquals( i++, dce.getSomeInt( cmh ));

            int id = dce.getEntryID();
            assertTrue( 0 <= id );
            entryIDList.add( id );

            // detect that we spawn blocks
            if ( id >> 16 != 0 ) break;
        }

        // verify access to all cached entries via entryID only
        i = 0;
        for ( int entryID : entryIDList ) {
            DummyCacheEntry dce = new DummyCacheEntry( entryID );

            assertEquals( myString, dce.getSomeString( cmh ) );
            assertEquals( i, dce.getSomeLong( cmh ) );
            assertEquals( i++, dce.getSomeInt( cmh ));
        }
    }

    @Test
    public void testEviction() {
        final String myString = "One long string to fill the buffer a little quicker";

        // figure out how many entries fit into a single block
        CacheMemoryHandler cmh         = new CacheMemoryHandler( 8 );  
        int                maxPerBlock = 0;

        handlersToClose.add( cmh );

        while( true ) {
            DummyCacheEntry dce = new DummyCacheEntry( cmh, myString, maxPerBlock, maxPerBlock );
            if ( dce.entryID >> 16 != 0 ) break;
            ++maxPerBlock;
        }

        assertTrue( 0 < maxPerBlock );

        cmh = new CacheMemoryHandler( 4 );
        TestListener tl = new TestListener( cmh );
        cmh.addEntryEvictionListener( tl );

        handlersToClose.add( cmh );

        ArrayList<Integer> createdEntryID = new ArrayList<>();

        // use all space in the single block
        for ( int i = 0; i < maxPerBlock; ++i ) {
            DummyCacheEntry dce = new DummyCacheEntry( cmh, myString, i, i );
            createdEntryID.add( dce.entryID );
            assertEquals( 0, tl.getEvicted().size() );
        }

        // this causes the eviction of the oldest entry (should be zero)
        DummyCacheEntry tooBig = new DummyCacheEntry( cmh, myString, maxPerBlock, maxPerBlock );
        createdEntryID.add( tooBig.entryID );
        assertEquals( 1, tl.getEvicted().size() );

        HashMap<String,Object> evictedData = tl.getEvicted().get( 0 );
        assertEquals( myString, evictedData.get( "string" ) );
        assertEquals( 0, evictedData.get( "int") );
        assertEquals( 0L, evictedData.get( "long" ) );
        createdEntryID.remove( 0 );

        // now access the new oldest - making it the most recently used again
        DummyCacheEntry reuse = new DummyCacheEntry( createdEntryID.get( 0 ) );
        assertEquals( myString, reuse.getSomeString( cmh ));
        assertEquals( 1, reuse.getSomeInt( cmh ) );
        assertEquals( 1L, reuse.getSomeInt( cmh ) );
        createdEntryID.add( createdEntryID.get( 0 ) );  
        createdEntryID.remove( 0 );

        // the new oldest - least recently used - is now entry 2. let's evict it
        DummyCacheEntry tooBig2 = new DummyCacheEntry( cmh, myString, maxPerBlock, maxPerBlock );
        createdEntryID.add( tooBig2.entryID );
        assertEquals( 2, tl.getEvicted().size() );

        evictedData = tl.getEvicted().get( 1 );
        assertEquals( myString, evictedData.get( "string" ) );
        assertEquals( 2, evictedData.get( "int") );
        assertEquals( 2L, evictedData.get( "long" ) );
        createdEntryID.remove( 0 );

        // now access every other entry (effectively bringing it to the front)
        List<Integer> copy = new ArrayList<Integer>( createdEntryID );
        copy.removeIf( new Predicate<Integer>() {
            @Override
            // filter out all even eventIDs
            public boolean test(Integer t) {
                return t % 2 == 0;
            }
        });
        // randomize order and acces each of the odd eventIDs
        Collections.shuffle( copy );
        for ( int entryID : copy ) {
            DummyCacheEntry access = new DummyCacheEntry( entryID );
            access.getSomeString( cmh );
        }

        // now produce 50% more entries and ensure that they don't evict any of
        // the 50% that we just touched
        tl.getEvicted().clear();
        for ( int i = 0; i < copy.size(); ++i ) {
            new DummyCacheEntry( cmh, myString, maxPerBlock, maxPerBlock );
        }

        // verify that the evicted ones are not part of our last touched
        assertEquals( copy.size(), tl.getEvicted().size() );
        for ( HashMap<String,Object> evictedEntry : tl.getEvicted() ) {
            int evictedID = (int)evictedEntry.get( "entryID" );
            assertFalse( copy.contains( evictedID ) );
            assertEquals( 0, evictedID % 2 );
        }
    }
}