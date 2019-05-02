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

import static java.lang.Math.min;

import java.io.Closeable;

import static java.lang.Math.max;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Manager for a specific amount of contiguous memory.
 * An instance of this class represents a memory block (optionally off heap), which can be
 * used in small subportions by getting subsized @c ByteBuffer instances from it. The class
 * also allows to free any of these smaller subportions again. Each subportion is represented
 * by an index. Before the memory of an index can be used, you have to synchronize on the 
 * instance of this class, receive the portion as @c ByteBuffer and then work within the limits
 * of that @c ByteBuffer. The reason for the index - to - @c ByteBuffer indirection is that
 * an instance of this class might compact its content, moving the relative positions of each
 * entry while keeping the index stable.
 * The allocated block of memory is used up from both ends. Each added antry's data is appended
 * from the front towards the end, while we add a "slot", to an array which is growing from the
 * end of the memory block towards the entry data. The memory block is full if these both meet.
 * Each slot is storing the offset of an entry. So, the slot of entry zero is using the last
 * four bytes (one int) of the memory allocation and it is storing the position of entry zero
 * within the memory block. The slot of entry one is using the 4 bytes before the slot of entry
 * zero, again storing the position of entry one. If we look up the position of an entry, we use
 * the entry index to find the slot (array in reverse order) and then go from there to the actual
 * entry data. This indexing method allows independent move of entries (i.e. upon compaction)
 * while the slot number remains stable.
 */
public class MemoryBlock implements Closeable {
    public  final static int MIN_ENTRY_SIZE = Long.BYTES;       ///< min allocation size
    private final static int DEFAULT_BLOCK_SIZE = 1024 * 1024;  ///< usedif no block size is provided
    private final static int MSB = (1 << (Integer.SIZE - 1));   ///< most significant integer bit
    private final static int SLOT_SIZE = Integer.BYTES;         ///< size of a slot (storing entry offset)
    private final static int ENTRY_HEADER_SIZE = Integer.BYTES; ///< header for each entry (just the entry size)  
    private final static int NONE = -1;                         ///< used for code readability below
    private final static int COMPACTED_SLOT = -1;               ///< indicator that a slot is invalid after compaction

    /// Used to iterate the different states of the private newEntry method
    private enum NewEntryTryMode { 
        Reused,               ///< initial call, allows search for reusable memory
        Append,               ///< ignore reusable memory, try to append to end 
        AppendAfterCompact    ///< retry append after compaction happened
    }

    private ByteBuffer rawBlock;            ///< the actual block of memory
    private int        blockSize;           ///< size of @c rawBlock in bytes
    private int        available;           ///< amount of bytes available (ignoring memory used by slots)
    private int        numSlots;            ///< total amount of slots (also compacted and reusable)
    private int        reusableSlots;       ///< number of reusable slots only
    private int        bottom;              ///< shows then end position (where next append happens)
    private int        nextReusableSlot;    ///< slot index of next reusable slot
    private boolean    hasCompactedSlots;   ///< indicator that some slots are invalid due to compaction

    /**
     * Creates a new @c MemoryBlock off heap (direct allocation) with 1MB size.
     */
    public MemoryBlock() {
        this( DEFAULT_BLOCK_SIZE, true );
    }

    /**
     * Creates a new @c MemoryBlock instance with the specified size.
     * A memory block should not be too small (there is some overhead assigned to each block) but
     * also not too large as it is a unit of compaction and you don't want to spend too much time
     * on compacting a single block. The default size is 1MB. The @c direct flag indicates if the
     * memory for the block is direct memory (off heap) or normal Java heap memory.
     * 
     * @param blockSize Size of the block in bytes
     * @param direct If @c true, the memory is allocated direct off heap
     */
    public MemoryBlock( int blockSize, boolean direct ) {
        checkArgument( blockSize > 1024, "The blockSize (%s) has a minimum of 1024 bytes.", blockSize );
        this.blockSize = blockSize;

        if ( direct )
            rawBlock = ByteBuffer.allocateDirect( blockSize );
        else 
            rawBlock = ByteBuffer.allocate( blockSize );

        available = blockSize;
        numSlots  = 0;
        bottom    = 0;
        nextReusableSlot = NONE;
        hasCompactedSlots = false;
    }

    /**
     * Dereferences all resources.
     */
    public void close() {
        rawBlock = null;
        available = 0;
        bottom = 0;
        nextReusableSlot = NONE;
    }

    /**
     * Returns the block size.
     * @return This is the value, that was provided to the constructor.
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Returns the maximum amount that could be used for the next allocation.
     * The method already subtracts the amount of memory that is internally required for
     * managing an allocation. So, the returned value represents the maximum size of a 
     * possible allocation (via @c newEntry).
     * 
     * @return The maximum available memory for an allocation in bytes
     */
    public int getAvailable() {
        int ret = available - (numSlots * SLOT_SIZE) - ENTRY_HEADER_SIZE - SLOT_SIZE;

        if ( ret < MIN_ENTRY_SIZE ) {
            ret = 0;
        }

        return ret;
    }

    /**
     * Allocates a new subportion of the memory block with specified size.
     * This method returns a new entry index for a new allocated sub portion in this @c MemoryBlock
     * instance. The memory, represented by the entry, can be accessed by the @c getBufferForIndex
     * method. Therefore, an entry index can be seen as a handle to memory in the block. Be aware
     * that the entry index is only unique within the same @c MemoryBlock instance!
     * 
     * @param size The amount of required memory in bytes
     * @return The entry index for this new allocation or -1 if no memory is available in this block
     */
    public synchronized int newEntry( int size ) {
        return newEntry( size, NewEntryTryMode.Reused );
    }

    /**
     * Returns the memory of a given entry.
     * The so released memory becomes available for later allocations (via @c newEntry) again, where
     * even the entry index as a handle might get reused. So, you should ensure that you get rid of
     * your reference of the entry index when calling this method.
     * 
     * @param slotIndex Entry index of the entry that is marked as reusable
     */
    public synchronized void freeEntry( int slotIndex ) {
        checkArgument( slotIndex >=0 && slotIndex < numSlots, "slotIndex out of bounds [0..%s]", numSlots - 1 );
        checkState( !isReusable( slotIndex ), "The specified slot %s was already deleted!", slotIndex );
        markReusable( slotIndex, true );

        if ( NONE == nextReusableSlot )
            nextReusableSlot = slotIndex;
        else    
            nextReusableSlot = min( nextReusableSlot, slotIndex );
    }

    /**
     * Returns the size of the specified entry.
     * 
     * @param slotIndex The entry index
     * @return The entry size in bytes
     */
    public synchronized int getSizeOfEntry( int slotIndex ) {
        int slotBytePos = blockSize - ((slotIndex + 1) * SLOT_SIZE);
        int entryOffset = rawBlock.getInt( slotBytePos ); 

        checkState( 0 <= entryOffset, "The entry at index %s was already deleted", slotIndex );
        return rawBlock.getInt( entryOffset );
    }

    /**
     * Returns the subsection of the cache block for the given index.
     * The method returns a slice of the internal, direct byte buffer. This slice can be used between
     * the position (zero) and its limit to read or write data into the cache position.
     * The cache content can be modified automatically by some other thread, i.e. due to compaction
     * of the block. So, whenever you get and access a slice via this method, you need to synchronize
     * on the @c CacheBlock by itself to avoid concurrent cache data changes.
     * Example:
     * <code>
     * CacheBlock cb = new CacheBlock();
     *   ...
     *   int myEntry = cb.newEntry( neededSize );
     *   ...
     * 
     *   synchronized(cb) {
     *     ByteBuffer mySlice = cb.getBufferForIndex( myEntry );
     *     for ( int i = 0; i < mySlice.limit(); ++i )
     *       mySlice.put( i, (byte)17 );
     *   }
     * </code>
     * 
     * @param slotIndex The index, received from @c newEntry
     * @return A sliced ByteBuffer for temporaty access to data under CacheBlock lock
     */
    public synchronized ByteBuffer getBufferForIndex( int slotIndex ) {
        checkArgument( slotIndex >=0 && slotIndex < numSlots, 
                       "slotIndex out of bounds [0..%s]", numSlots - 1 );
        checkState( !isReusable( slotIndex ), 
                    "The entry at index %s was already deleted", slotIndex );

        int slotBytePos = blockSize - ((slotIndex + 1) * SLOT_SIZE);
        int entryOffset = rawBlock.getInt( slotBytePos ); 

        checkState( entryOffset != COMPACTED_SLOT, 
                    "The entry at index %s was already deleted (and compacted)", slotIndex );        

        int entryLen = rawBlock.getInt( entryOffset );
        rawBlock.position( entryOffset + ENTRY_HEADER_SIZE );
        ByteBuffer ret = rawBlock.slice();
        ret.limit( entryLen );
        
        return ret;
    }

    /**
     * Returns the total amount of slots.
     * The slots are growing from the end of the memory block towards the beginning until they
     * meet with the actual entry data. Slots are only marked reusable but they are never removed,
     * once they were allocated. Therefore, this value represents the slot array size at the end
     * of the memory block (in entries).
     * 
     * @return The amount of slots in this block
     */
    public synchronized int getSlotCount() {
        return numSlots;
    }

    /**
     * Returns the amount of reusable slots.
     * Reusable slots are entries that were returned via the @c freeEntry and which were not
     * picked up yet by subsequent allocations. 
     * 
     * @return The amount of currently reusable slots
     */
    public synchronized int getReusableSlotCount() {
        return reusableSlots;
    }

    /**
     * Compacts the memory block.
     * The memory block might get fragmented due to memory that is returned via @c freeEntry to
     * this instance. This method can be called to defragment the whole memory block (while keeping 
     * the entry indexes stable). You can call this method "proactively" but it is also called 
     * automatically if an allocation fails due to the unavailability of a large enough range
     * within this memory block. So, calling it proactively isn't really required other than for
     * performance optimization purposes.
     */
    public synchronized void compact() {
        if ( reusableSlots > 0 ) {      // check that there is actually something to compact
            int currentCopyPos = 0;
            int lastSlotWithData = NONE;
    
            // iterate all slots
            for ( int slotIdx = 0; slotIdx < numSlots; ++slotIdx ) {
                int slotBytePos = blockSize - ((slotIdx + 1) * SLOT_SIZE);
                int entryOffset = rawBlock.getInt( slotBytePos ); 
    
                if ( 0 <= entryOffset ) {
                    // slot contains valid data
                    int entryLength = rawBlock.getInt( entryOffset );
    
                    // valid entry, move it up
                    if ( entryOffset != currentCopyPos ) {
                        if ( rawBlock.hasArray() ) {
                            System.arraycopy( rawBlock.array(), entryOffset, 
                                              rawBlock.array(), currentCopyPos, 
                                              entryLength + ENTRY_HEADER_SIZE );
                        }
                        else {
                            // direct allocated buffers don't provide an array
                            moveDirect( entryOffset, currentCopyPos, entryLength + ENTRY_HEADER_SIZE );
                        }
    
                        // update the offset in the slot entry
                        rawBlock.putInt( slotBytePos, currentCopyPos );
                    }
    
                    currentCopyPos += entryLength + ENTRY_HEADER_SIZE;
                    lastSlotWithData = slotIdx;
                }
                else {
                    // mark the slot as compacted
                    rawBlock.putInt( slotBytePos, COMPACTED_SLOT );
                    hasCompactedSlots = true;
                }
            }
    
            // forget about the compacted slots at the very end
            if ( lastSlotWithData + 1 < numSlots ) {
                numSlots = lastSlotWithData + 1;
            }
    
            bottom = currentCopyPos;
            reusableSlots = 0;
    
            // sanity check that the bottom is where we expect it
            assert( available == (blockSize - bottom) );
        }
    }

    /**
     * Helper method to perform the actual allocation.
     * The method might perform multiple retries in the following order: First, it tries to reuse
     * an entry that was previously returned via @c freeEntry (if there was any). If it can't find
     * a reusable entry, it calls itself again in append mode, asking to add a new entry. If this
     * fails, it runs a compaction and then tries to append again.
     * 
     * @param size The amount of required memory in bytes
     * @param mode Current state (allocation mode)
     * @return The entry index for the allocation or -1 if no memory was found
     */
    private int newEntry( int size, NewEntryTryMode mode ) {
        int slot = NONE;

        checkArgument( 0 < size , "The size %s has to be > zero", size );
        size = max( size, MIN_ENTRY_SIZE );

        if ( NONE                               == nextReusableSlot || 
             NewEntryTryMode.Append             == mode              || 
             NewEntryTryMode.AppendAfterCompact == mode                 ) {
            // check that there is still enough room before hitting the slot array
            if ( bottom + ENTRY_HEADER_SIZE + size <= (blockSize - ((numSlots + 1) * SLOT_SIZE)) ) {
                slot = nextSlot();

                // store the offset to the entry in the slot position 
                int slotBytePos = blockSize - ((slot + 1) * SLOT_SIZE);
                rawBlock.putInt( slotBytePos, bottom );

                // store the length in the header of the new entry
                rawBlock.putInt( bottom, size );
                bottom += ENTRY_HEADER_SIZE + size;
                available -= ENTRY_HEADER_SIZE + size;
            } 
            else if ( NewEntryTryMode.Append == mode ) {
                // we have free space (but not large enough) and not enough to append either
                compact();
                slot = newEntry( size, NewEntryTryMode.AppendAfterCompact );
            }
        }
        else {
            // search for reusable entry (previously freed)
            for ( int slotIdx = nextReusableSlot; slotIdx < numSlots; ++slotIdx ) {
                if ( isReusable( slotIdx ) ) {
                    int entryOffset = getEntryOffset( slotIdx );
                    int entrySize   = rawBlock.getInt( entryOffset );

                    if ( size <= entrySize ) {
                        markReusable( slotIdx, false );

                        // update the size of the reused entry with real size
                        rawBlock.putInt( entryOffset, size );

                        // push the first reusable to next reusable
                        if ( slotIdx == nextReusableSlot ) 
                            nextReusableSlot = findNextReusable();

                        slot = slotIdx;
                        break;
                    }
                }
            }

            // didn't find any reusable slot, so try another time via append to end
            if ( NONE == slot ) {
                slot = newEntry( size, NewEntryTryMode.Append );
            }
        }

        return slot;
    }

    /**
     * Helper to scan for the next reusable slot entry that is reusable.
     * Used by @c newEntry to adjust the position of the first reusable slot entry.
     * 
     * @return The next reusable slot entry
     */
    private int findNextReusable() {
        for ( int slotIdx = nextReusableSlot + 1; slotIdx < numSlots; ++slotIdx ) {
            int slotBytePos = blockSize - ((slotIdx + 1) * SLOT_SIZE);
            int entryPos    = rawBlock.getInt( slotBytePos );
            if ( (entryPos & MSB) != 0 && COMPACTED_SLOT != entryPos ) {
                return slotIdx;
            }
        }

        return NONE;
    }

    /**
     * Returns the byte offset of an entry.
     * The method calcualtes the slot position (growing from the end of the memory block) and 
     * determines the entries's offset, stored in that slot. Even if the slot value has the MSB
     * set (to indicate that it is reusable), this method will return the raw entry offset.
     * 
     * @param slotIdx Zero based index of the entry for which we need the position
     * @return The byte position of the entry within this block
     */
    private int getEntryOffset( int slotIdx ) {
        assert( slotIdx >= 0 && slotIdx < numSlots );
        int slotBytePos = blockSize - ((slotIdx + 1) * SLOT_SIZE);
        int entryOffset = rawBlock.getInt( slotBytePos ); 

        return (entryOffset & ~MSB);   // mask out most significant bit
    }

    /**
     * Figures out if a particular slot entry is reusable.
     * A slot is reusable if it was marked so (MSB of the slot value is set) and it therefore
     * is pointing to valid but unused memory within the block. These entries are automatically
     * invalidated upon compaction, in which case this method would indicate that it is not
     * reusable.
     * 
     * @param slotIdx The entry to check
     * @return True if the slot with its entry is reusable
     */
    private boolean isReusable( int slotIdx ) {
        assert( slotIdx >= 0 && slotIdx < numSlots );
        int slotBytePos = blockSize - ((slotIdx + 1) * SLOT_SIZE);
        int entryOffset = rawBlock.getInt( slotBytePos ); 

        // check if the most significant bit is set
        return (entryOffset & MSB) != 0 && entryOffset != COMPACTED_SLOT;
    }

    /**
     * Helper to mark or unmark an entry as reusable.
     * We mark slot entries as reusable (when they are deleted) by setting the most significant
     * bit of the entry offset. This (resulting in a negative number w/o loosing the actual 
     * position) allows us to later find and reuse the entry. The method also maintains the
     * amount of reusabel slots.
     * 
     * @param slotIdx The entry index to mark or unmark reusable 
     * @param reusable If true, the entry is marked reusable
     */
    private void markReusable( int slotIdx, boolean reusable ) {
        assert( slotIdx >= 0 && slotIdx < numSlots );
        int slotBytePos = blockSize - ((slotIdx + 1) * SLOT_SIZE);
        int entryOffset = rawBlock.getInt( slotBytePos ); 

        assert( ((entryOffset & MSB) == 0) == reusable && entryOffset != COMPACTED_SLOT );

        if ( reusable ) {
            available += rawBlock.getInt( entryOffset ) + ENTRY_HEADER_SIZE;
            entryOffset |= MSB;  // set most significant bit
            ++reusableSlots;
        }
        else {
            entryOffset &= ~MSB;  // clear most significant bit
            available -= rawBlock.getInt( entryOffset ) + ENTRY_HEADER_SIZE;
            --reusableSlots;

            assert( 0 <= reusableSlots );
        }

        rawBlock.putInt( slotBytePos, entryOffset );
    }

    /**
     * Searches for the next valid slot/entry number.
     * This is normally just the last higher slot number in append case but after compaction,
     * this helper might reuse some of the compacted (originally reuseable) slots.
     * 
     * @return The next slot number to use
     */
    private int nextSlot() {
        int ret = NONE;

        if ( hasCompactedSlots ) {
            // try to find a slot that can be reused ofter compaction
            for ( int slotIndex = 0; slotIndex < numSlots; ++slotIndex ) {
                int slotBytePos = blockSize - ((slotIndex + 1) * SLOT_SIZE);
                int entryOffset = rawBlock.getInt( slotBytePos ); 

                if ( COMPACTED_SLOT == entryOffset ) {
                    ret = slotIndex;
                    break;
                }
            }

            if ( NONE == ret ) {
                hasCompactedSlots = false;
                ret = numSlots++;
            }
        }
        else
            ret = numSlots++;
        
        return ret;
    }

    /**
     * Moves a chunk of memory within the raw memory block.
     * This method is required if we have direct (off heap) allocation. In that case, we can't
     * get that memory as an array and therefore can't use traditional arraycopy to move data
     * around in the case of a compaction.
     * 
     * @param from Starting position (to copy from)
     * @param to Target position (to copy to)
     * @param length Amount of data to move (actually copy) in bytes
     */
    private void moveDirect( int from, int to, int length ) {
        byte[] copyBuffer = new byte[512];
        int    copied     = 0;

        // copy/move in chunks of 512 bytes
        while( copied < length ) {
            int toCopy = min( length - copied, copyBuffer.length );

            rawBlock.position( from );
            rawBlock.get( copyBuffer, 0, toCopy );
            rawBlock.position( to );
            rawBlock.put( copyBuffer, 0, toCopy );

            copied += toCopy;
            from   += toCopy;
            to     += toCopy;
        }
    }
}