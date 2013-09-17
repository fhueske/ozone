/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.io.pax;

import java.io.IOException;
import java.io.OutputStream;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.io.pax.io.ByteDataOutputStream;
import eu.stratosphere.pact.common.io.pax.io.UnSyncByteArrayOutputStream;
import eu.stratosphere.pact.common.io.pax.selection.bloomfilter.BloomFilter;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Container for the header of a row group.
 * <p/>
 * The header of a row group saves all information needed to retrieve and efficiently work with the row group.
 * To efficiently save the length of the fields in the columns,
 * run length encoding is used for fields with the same length.
 * <p/>
 * <p/>
 * For the concrete structure, see the paxformat.eps figure in the resources folder.
 *
 */
public class OutputHeader {

    /**
     * The desired error rate of the bloom filter.
     */
    public static final double BLOOM_FILTER_ERROR_RATE = 0.1;

    /**
     * The expected values to be inserted into the bloom filter.
     */
    public static final int BLOOM_FILTER_EXPECTED_VALUES = 40000;

    public static final byte SORTED_ASC_FLAG = 0x01;

    public static final byte SORTED_DSC_FLAG = 0x02;

    public static final byte BLOOM_FILTER_FLAG = 0x04;

    public static final byte LOW_HIGH_VALUES = 0x08;

    /**
     * 16 byte block for error checks and synchronization while reading row groups.
     */
    public static final byte[] SYNC_BLOCK; // 16 Byte row group sync block

    static {
        try {
            MessageDigest digester = MessageDigest.getInstance("MD5");
            long time = System.currentTimeMillis();
            digester.update((new UID() + "@" + time).getBytes());
            SYNC_BLOCK = digester.digest();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // header data

    /**
     * The size of the header in bytes. This value is updated while
     * new data is added to the columns.
     * <p/>
     * The final value is set when
     * {@link OutputHeader#finish()} is called.
     */
    private int sizeInBytes;

    private int columnsSizeInBytes;

    /**
     * Specifies the number of bytes which are added at the end of the row group to
     * archive row groups fit always in one block only.
     */
    private int paddingSize;

    /**
     * The position of the column which is sorted, or -1.
     */
    private final Map<Integer, Sorter> sortedColumns;

    /**
     * The number of records in this row group. Increases while values are added to the row group.
     * <p/>
     * The final value is set when
     * {@link OutputHeader#finish()} is called.
     */
    private int records;


    private final Map<Integer, BloomFilter> bloomFilterColumns;

    /**
     * The number of columns in this row group.
     */
    private final int columns;

    /**
     * Number of bytes used by the uncompressed column values.
     */
    private final int[] uncompressedBytesPerColumn;

    /**
     * Number of bytes used by the compressed column values.
     */
    private final int[] compressedBytesPerColumn;

    /**
     * The actual compressed values of the columns.
     */
    private final UnSyncByteArrayOutputStream[] bytesPerField;

    /**
     * The lowest / highest keys of the columns (if they are of type {@link Key}).
     */
    private final ByteDataOutputStream[] highLowKeys;

    // used for efficient data representation

    /**
     * The length of the value previously added in the column (for RLE).
     */
    private final int[] previousFieldLength;

    /**
     * The number of consecutive values with the same length added in the columns (for RLE).
     */
    private final int[] sameFieldLengthBuffer;

    /**
     * Creates a new Header for a indexed pax format.
     * <p/>
     * While writing a ipf, the header class should be reused by the row groups.
     *
     * @param columns            the number of columns in the row group.
     * @param sortedColumns      the sorted columns in the row group
     * @param bloomFilterColumns the columns with bloom filter
     */
    public OutputHeader(int columns, Map<Integer, Order> sortedColumns, List<Integer> bloomFilterColumns) {
        uncompressedBytesPerColumn = new int[columns];
        compressedBytesPerColumn = new int[columns];
        bytesPerField = new UnSyncByteArrayOutputStream[columns];
        previousFieldLength = new int[columns];
        sameFieldLengthBuffer = new int[columns];
        highLowKeys = new ByteDataOutputStream[columns];
        for (int i = 0; i < columns; i++) {
            bytesPerField[i] = new UnSyncByteArrayOutputStream();
            previousFieldLength[i] = -1;
            highLowKeys[i] = new ByteDataOutputStream();
        }
        this.columns = columns;

        this.sortedColumns = new HashMap<Integer, Sorter>(sortedColumns.size());
        for (Map.Entry<Integer, Order> entry : sortedColumns.entrySet()) {
            this.sortedColumns.put(entry.getKey(), new Sorter(entry.getValue()));
        }

        this.bloomFilterColumns = new HashMap<Integer, BloomFilter>(bloomFilterColumns.size());
        for (int position : bloomFilterColumns) {
            this.bloomFilterColumns.put(position, new BloomFilter(BLOOM_FILTER_ERROR_RATE, BLOOM_FILTER_EXPECTED_VALUES));
        }

        setDefaultSize();
    }

    /**
     * Writes the header to given stream.
     * For the header's disk representation see the class java doc.
     *
     * @param stream the stream the header is written to
     */
    public void write(OutputStream stream) throws IOException {
        // check if all RLE buffers are written to the byte streams
        // if not update columns, before flush to stream
        for (int i = 0; i < columns; i++) {
            if (sameFieldLengthBuffer[i] < -1) {
            	Utils.writeInt(bytesPerField[i], sameFieldLengthBuffer[i]);
            }
        }

        // write to stream
        stream.write(SYNC_BLOCK);
        Utils.writeInt(stream, paddingSize);
        Utils.writeInt(stream, sortedColumns.size());
        Utils.writeInt(stream, bloomFilterColumns.size());
        Utils.writeInt(stream, records);
        Utils.writeInt(stream, columns);
        for (int i = 0; i < columns; i++) {
            final boolean sorted = sortedColumns.containsKey(i);
            final boolean bloom = bloomFilterColumns.containsKey(i);
            final boolean lhKeys = highLowKeys[i].size() > 0;

            Utils.writeInt(stream, uncompressedBytesPerColumn[i]);
            Utils.writeInt(stream, compressedBytesPerColumn[i]);
            Utils.writeInt(stream,
                    1 + // flag
                            (sorted ? sortedColumns.get(i).getSizeOnDisk() : 0) + // sorted
                            (bloom ? bloomFilterColumns.get(i).getSizeOnDisk() : 0) + // bloom
                            (lhKeys ? highLowKeys[i].size() : 0) + // LH
                            bytesPerField[i].size()); // values
            // feature flag
            stream.write(
                    (sorted ? sortedColumns.get(i).getOrder() == Order.ASCENDING ? SORTED_ASC_FLAG : SORTED_DSC_FLAG : 0x00) |
                            (bloom ? BLOOM_FILTER_FLAG : 0x00) |
                            (lhKeys ? LOW_HIGH_VALUES : 0x00)
            );
            // sorting
            if (sorted) {
                for (int row : sortedColumns.get(i).getSortedRows()) {
                	Utils.writeInt(stream, row);
                }
            }
            // bloom filter
            if (bloom) {
                bloomFilterColumns.get(i).write(stream);
            }
            if (lhKeys) {
                // high low keys
                // writeInt(stream, highLowKeys[i].size());
                stream.write(highLowKeys[i].getData());
            }

            stream.write(bytesPerField[i].toByteArray());
        }
    }

    /**
     * Resets the header, all saved field data is deleted.
     */
    public void reset() {
        paddingSize = 0;
        records = 0;

        for (Sorter sorter : sortedColumns.values()) {
            sorter.reset();
        }
        for (BloomFilter filter : bloomFilterColumns.values()) {
            filter.reset();
        }

        for (int i = 0; i < columns; i++) {
            uncompressedBytesPerColumn[i] = 0;
            compressedBytesPerColumn[i] = 0;
            bytesPerField[i].reset();
            previousFieldLength[i] = -1;
            sameFieldLengthBuffer[i] = 0;
            highLowKeys[i].reset();
        }

        setDefaultSize();
        columnsSizeInBytes = 0;
    }

    /**
     * Increases the record count for this row group.
     */
    public void increaseRecords() {
        records++;
    }


    /**
     * Sets the compressed length of the column on disk.
     *
     * @param position the position of the column
     * @param length   the compressed length of the column
     */
    public void setCompressedBytesPerColumn(int position, int length) {
        compressedBytesPerColumn[position] = length;
        synchronized (this) {
            columnsSizeInBytes += length;
        }
    }

    /**
     * Updates the length fields for the column at the specifed position.
     *
     * @param position    the position of the column the field belongs to.
     * @param fieldLength the length of the field
     * @throws java.io.IOException
     */
    public void updateColumn(int position, Value field, int fieldLength) throws IOException {
        // set bloom filter
        if (bloomFilterColumns.containsKey(position)) {
            bloomFilterColumns.get(position).add(field.toString());
        }

        if (sortedColumns.containsKey(position)) {
            sortedColumns.get(position).add(records, (Key) Utils.copy(field));
            //sizeInBytes += 4;
        }

        uncompressedBytesPerColumn[position] += fieldLength;
        // check if previous fieldLength had the same length
        if (fieldLength == previousFieldLength[position]) {
            // decrement length buffer and add to header compressedSize
            // done here to ensure right header compressedSize
            if (sameFieldLengthBuffer[position] == -1) {
                sizeInBytes += 4;
            }
            sameFieldLengthBuffer[position]--;
        } else {
            if (sameFieldLengthBuffer[position] < -1) {
            	Utils.writeInt(bytesPerField[position], sameFieldLengthBuffer[position]);
            }
            Utils.writeInt(bytesPerField[position], fieldLength);
            previousFieldLength[position] = fieldLength;
            sameFieldLengthBuffer[position] = -1;
            sizeInBytes += 4;
        }
    }

    /**
     * The size of the header on disk in bytes.
     * <p/>
     * Only if {@link OutputHeader#finish()} is called first, the correct value is returned!
     *
     * @return the size of the header on disk in bytes.
     */
    public int sizeOnDisk() {
        return sizeInBytes;
    }

    /**
     * The size of the columns on disk in bytes.
     * <p/>
     * Only if {@link OutputHeader#finish()} is called first, the correct value is returned!
     *
     * @return the size of the columns on disk in bytes.
     */
    public int columnsSizeInBytes() {
        return columnsSizeInBytes;
    }

    /**
     * Sets the length of the trailing null bytes added to the end of the row group.
     *
     * @param paddingSize the padding length
     */
    public void setPaddingSize(int paddingSize) {
        this.paddingSize = paddingSize;
    }

    /**
     * Set default header size.
     * <p/>
     * Dynamic fields are increased as values are added.
     */
    private void setDefaultSize() {
        sizeInBytes = 16         // sync
                + 4             // padding compressedSize
                + 4             // sort column
                + 4             // bloom filter column
                + 4             // records
                + 4             // columns
                + (4 * columns) // bytes per uncompressed column
                + (4 * columns) // bytes per compressed column
                + (4 * columns) // skipable bytes length
                + (columns);            // flags

        for (BloomFilter filter : bloomFilterColumns.values()) {
            sizeInBytes += filter.getSizeOnDisk();
        }

    }

    /**
     * Sets the lowest / highest values in the specified column.
     *
     * @param position the column
     * @param low      the lowest value in the column
     * @param high     the highest value in the column
     * @throws java.io.IOException
     */
    public void setLowHighKeys(int position, Key low, Key high) throws IOException {
        if (high != null && low != null) {
            high.write(highLowKeys[position]);
            low.write(highLowKeys[position]);
            synchronized (this) {
                sizeInBytes += highLowKeys[position].size();
            }
        }
    }

    /**
     * Marks this row group as finished and ready to be flushed to disk.
     * All remaining data is written, and the size is updated.
     */
    public void finish() {
        for (Sorter sorter : sortedColumns.values()) {
            sorter.finish();
            sizeInBytes += sorter.getSizeOnDisk();
        }
    }

    /**
     * The number of records in the row group.
     *
     * @return the number of records in the row group.
     */
    public int records() {
        return records;
    }
}
