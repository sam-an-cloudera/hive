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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.lang.Math.max;

import java.nio.ByteBuffer;
import org.junit.Test;

public class TestMemoryBlock {
    private final static int OVERHEAD_PER_ENTRY = Integer.BYTES * 2;

    @Test
    public void testSimpleAdd() throws Exception {
        final int BB_SIZE_1 = 100;
        final int BB_SIZE_2 = 150;

        MemoryBlock mb = new MemoryBlock();

        assertEquals( mb.getBlockSize() - OVERHEAD_PER_ENTRY, mb.getAvailable() );
        int idx1 = mb.newEntry( BB_SIZE_1 );
        assertEquals( 0, idx1 );

        synchronized( mb ) {
            assertEquals( mb.getSizeOfEntry( idx1 ), BB_SIZE_1 );

            ByteBuffer bb1 = mb.getBufferForIndex( idx1 );
            assertNotNull( bb1 );
            assertEquals( BB_SIZE_1, bb1.limit() );
            assertEquals( mb.getBlockSize() - (OVERHEAD_PER_ENTRY * 2) - BB_SIZE_1, mb.getAvailable() );
            fillByteBuffer( bb1, (byte)BB_SIZE_1 );
        }

        int idx2 = mb.newEntry( BB_SIZE_2 );
        assertEquals( 1, idx2 );

        synchronized( mb ) {
            assertEquals( mb.getSizeOfEntry( idx2 ), BB_SIZE_2 );

            ByteBuffer bb2 = mb.getBufferForIndex( idx2 );
            assertNotNull( bb2 );
            assertEquals( BB_SIZE_2, bb2.limit() );
            assertEquals( mb.getBlockSize() - (OVERHEAD_PER_ENTRY * 3) - BB_SIZE_1 - BB_SIZE_2, mb.getAvailable() );
            fillByteBuffer( bb2, (byte)BB_SIZE_2 );
        }

        verifyByteBuffer( mb, idx1, (byte)BB_SIZE_1 );
        verifyByteBuffer( mb, idx2, (byte)BB_SIZE_2 );
    }

    @Test
    public void testFill() throws Exception {
        final int BB_SIZE_1 = 1931;
        final int BB_SIZE_2 = 4129;

        MemoryBlock mb = new MemoryBlock();

        int expectedSize = 0;
        int seq = 0;
        int whereStopped = 0;

        while( true ) {
            int newSize = expectedSize + BB_SIZE_1 + OVERHEAD_PER_ENTRY;

            if ( newSize <= mb.getBlockSize() ) {
                int idx = mb.newEntry( BB_SIZE_1 );

                assertEquals( seq++, idx );
                synchronized( mb ) {
                    ByteBuffer bb = mb.getBufferForIndex( idx );
                    fillByteBuffer( bb, (byte)seq );
                }

                expectedSize += BB_SIZE_1 + OVERHEAD_PER_ENTRY;
                assertEquals( expectedSize + OVERHEAD_PER_ENTRY, mb.getBlockSize() - mb.getAvailable() );
            }
            else {
                whereStopped = 1;
                break;
            }

            newSize = expectedSize + BB_SIZE_2 + OVERHEAD_PER_ENTRY;   

            if ( newSize <= mb.getBlockSize() ) {
                int idx = mb.newEntry( BB_SIZE_2 );

                assertEquals( seq++, idx );
                synchronized( mb ) {
                    ByteBuffer bb = mb.getBufferForIndex( idx );
                    fillByteBuffer( bb, (byte)seq );
                }

                expectedSize += BB_SIZE_2 + OVERHEAD_PER_ENTRY;
                assertEquals( expectedSize + OVERHEAD_PER_ENTRY, mb.getBlockSize() - mb.getAvailable() );
            }
            else {
                whereStopped = 2;
                break;
            }
        }

        if ( whereStopped == 1 ) {
            assertTrue( "Wasn't supposed to have space for BB_SIZE_1", mb.getAvailable() < BB_SIZE_1 );
            assertEquals( -1, mb.newEntry( BB_SIZE_1 ) );
        }
        else {
            assertTrue( "Wasn't supposed to have space for BB_SIZE_2", mb.getAvailable() < BB_SIZE_2 );
            assertEquals( -1, mb.newEntry( BB_SIZE_2 ) );
        }

        // validate content
        for( int idx = 0; idx < seq; ++idx ) {
            verifyByteBuffer( mb, idx, (byte)(idx + 1) );
        }
    }

    @Test
    public void testSpaceReuse() throws Exception {
        final int BB_SIZE = 4129;

        MemoryBlock mb = new MemoryBlock();

        int seq = 0;

        // fill it up
        while( true ) {
            int idx = mb.newEntry( BB_SIZE );

            if ( 0 <= idx ) {
                assertEquals( seq, idx );
                synchronized( mb ) {
                    ByteBuffer bb = mb.getBufferForIndex( idx );
                    fillByteBuffer( bb, (byte)seq );
                }

                ++seq;
            }
            else {
                break;
            }
        }

        assertTrue( "Wasn't supposed to have space for BB_SIZE_1", mb.getAvailable() < BB_SIZE );

        // now delete every other entry
        int deleted = 0;
        for ( int idx = 0; idx < seq; idx += 2 ) {
            mb.freeEntry( idx );
            ++deleted;
            
            // check that a getBufferForIndex fails for the deleted entry
            try {
                mb.getBufferForIndex( idx );
                fail( "This should have produced an IllegalStateException" );
            }
            catch( Throwable thr ) {
                assertTrue( thr instanceof IllegalStateException );
            }
        }

        // deleted space should be available again
        assertTrue( deleted * (BB_SIZE + OVERHEAD_PER_ENTRY) <= mb.getAvailable() );

        // reuse the space
        seq = 0;
        for ( int fillGap = 0; fillGap < deleted; ++fillGap ) {
            int idx = mb.newEntry( BB_SIZE );

            assertEquals( seq, idx );
            seq += 2;

            synchronized( mb ) {
                ByteBuffer bb = mb.getBufferForIndex( idx );
                fillByteBuffer( bb, (byte)0xFF );
            }
        }

        // verify the content of all entries (reused have FF as content)
        for ( int slotIndex = 0; slotIndex < mb.getSlotCount(); ++slotIndex ) {
            if ( slotIndex % 2 == 0 ) 
                verifyByteBuffer( mb, slotIndex, (byte)0xFF );
            else 
                verifyByteBuffer( mb, slotIndex, (byte)slotIndex );
        }

        int tooFull = mb.newEntry( BB_SIZE );
        int avail   = mb.getAvailable();
        assertEquals( -1, tooFull );
        assertTrue( avail < BB_SIZE );

        if ( 0 != avail ) {
            int theRest = mb.newEntry( avail );

            assertNotEquals( -1, theRest );
            synchronized( mb ) {
                ByteBuffer bb = mb.getBufferForIndex( theRest );
                fillByteBuffer( bb, (byte)((theRest % 2 == 0)?0xFF:theRest) );
            }
        }

        assertEquals( 0, mb.getAvailable() );

        // verify the content of all entries and appended
        for ( int slotIndex = 0; slotIndex < mb.getSlotCount(); ++slotIndex ) {
            if ( slotIndex % 2 == 0 ) 
                verifyByteBuffer( mb, slotIndex, (byte)0xFF );
            else 
                verifyByteBuffer( mb, slotIndex, (byte)slotIndex );
        }
    }

    @Test
    public void testImplicitCompact() {
        final int BB_SIZE_1 = 1703;
        final int BB_SIZE_2 = 4129;

        MemoryBlock mb = new MemoryBlock();

        // fill it up
        while( true ) {
            int idx = mb.newEntry( BB_SIZE_1 );

            if ( 0 <= idx ) {
                synchronized( mb ) {
                    ByteBuffer bb = mb.getBufferForIndex( idx );
                    fillByteBuffer( bb, (byte)idx );
                }
            }
            else {
                break;
            }
        }

        assertTrue( mb.getAvailable() < BB_SIZE_1 );

        // delete every other entry
        for ( int idx = 1; idx < mb.getSlotCount(); idx += 2 ) {
            mb.freeEntry( idx );
        }

        // now there should be enough space again fo the larger BB_SIZE_2 block but
        // the memory is fragmented (only BB_SIZE_1 chunks available). So, if we request
        // a BB_SIZE_2 chunk, the block will have to be compacted before the request can
        // be satisfied.
        assertTrue( mb.getAvailable() >= max( BB_SIZE_1, BB_SIZE_2 ) );

        int blockAfterCompact = mb.newEntry( BB_SIZE_2 );
        assertNotEquals( -1, blockAfterCompact );

        synchronized( mb ) {
            ByteBuffer bb = mb.getBufferForIndex( blockAfterCompact );
            fillByteBuffer( bb, (byte)blockAfterCompact );
        }

        // verify content after compaction
        int accessedCompactedOrDeleted = 0;
        for ( int slotIndex = 0; slotIndex < mb.getSlotCount(); ++slotIndex ) {
            try {
                mb.getBufferForIndex( slotIndex );  // to ensure that exception is thrown on deleted
                verifyByteBuffer( mb, slotIndex, (byte)slotIndex );
            }
            catch( IllegalStateException ise ) {
                // the compacted slots cause an exception upon index access (were deleted before)
                ++accessedCompactedOrDeleted;
            }
        }

        assertEquals( (mb.getSlotCount() / 2) - 1, accessedCompactedOrDeleted );
    }

    private void fillByteBuffer( ByteBuffer bb, byte value ) {
        for ( int i = 0; i < bb.limit(); ++i )  
            bb.put( i, value );
    } 

    private void verifyByteBuffer( MemoryBlock mb, int idx, byte value ) {
        synchronized( mb ) {
            ByteBuffer bb = mb.getBufferForIndex( idx );

            for ( int i = 0; i < bb.limit(); ++i ) 
                assertEquals( value, bb.get( i ) );
        }
    }

}