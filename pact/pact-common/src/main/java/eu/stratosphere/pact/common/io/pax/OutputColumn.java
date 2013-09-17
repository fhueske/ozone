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

import eu.stratosphere.pact.common.io.pax.io.ByteDataOutputStream;
import eu.stratosphere.pact.common.io.pax.io.compression.CompressionCodecs;
import eu.stratosphere.pact.common.io.pax.io.compression.ICompressor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * A column in the row group.
 * <p/>
 * The data of the column is compressed before it is written to disk.
 *
 */
public class OutputColumn implements Comparable<OutputColumn> {

    /**
     * Stream to the byte buffer containing the values of this column.
     */
    private final ByteDataOutputStream output;

    /**
     * The type of this column.
     */
    private final Class<? extends Value> valueClass;

    /**
     * The position of this column in the record.
     */
    private final int columnPosition;

    /**
     * True if this column is of type {@link Key}.
     */
    private final boolean keyColumn;

    /**
     * The lowest value in this column (if its of type {@link Key}).
     */
    private Key low;

    /**
     * The highest value in this column (if its of type {@link Key}).
     */
    private Key high;

    /**
     * The byte buffer holding the compressed values of the column.
     */
    private byte[] compressedBuffer;

    /**
     * Compressor used to compress the column values.
     */
    private final ICompressor compressor;

    /**
     * Creates an empty column.
     *
     * @param valueClass        the class of the literals in this column
     * @param columnPosition    the position of the column in the record
     * @param initialBufferSize the initial size of the underlying byte buffer
     */
    public OutputColumn(Class<? extends Value> valueClass, int columnPosition, int initialBufferSize) {
        this.valueClass = valueClass;
        this.keyColumn = Key.class.isAssignableFrom(valueClass);
        this.columnPosition = columnPosition;
        this.output = new ByteDataOutputStream(initialBufferSize);
        this.compressor = CompressionCodecs.getGZip();
    }

    /**
     * Creates an empty column.
     *
     * @param valueClass        the class of the literals in this column
     * @param columnPosition    the position of the column in the record
     * @param initialBufferSize the initial size of the underlying byte buffer
     * @param compressor        the compressor used to compress the column data
     */
    public OutputColumn(Class<? extends Value> valueClass, int columnPosition, int initialBufferSize, ICompressor compressor) {
        this.valueClass = valueClass;
        this.keyColumn = Key.class.isAssignableFrom(valueClass);
        this.columnPosition = columnPosition;
        this.output = new ByteDataOutputStream(initialBufferSize);
        this.compressor = compressor;
    }

    /**
     * Adds a value to the column.
     *
     * @param value the value added.
     * @return the size of the value in bytes.
     * @throws IOException
     */
    public int addValue(Value value) throws IOException {
        setLowHighValues(value);
        value.write(output);
        return output.getLastBytesWritten();
    }

    /**
     * The value type of the column.
     *
     * @return the value type of the column
     */
    public Class<? extends Value> getValueClass() {
        return valueClass;
    }

    /**
     * The columnPositionInRecord of the column.
     *
     * @return the columnPositionInRecord of the column
     */
    public int getColumnPosition() {
        return columnPosition;
    }

    /**
     * Resets the byte buffer of the column.
     * The column contains no values afterwards.
     */
    public void resetBuffer() {
        output.reset();
        low = null;
        high = null;
    }

    /**
     * The compressedSize of the column in bytes.
     * <p/>
     * IMPORTANT: {@link OutputColumn#compress()} has to be called first
     * in order to retrieve the correct compressedSize of the compressed column.
     *
     * @return the compressedSize of the column in bytes.
     */
    public int compressedSize() {
        return compressedBuffer.length;
    }

    /**
     * Compresses the whole column.
     */
    public void compress() {
        compressedBuffer = compressor.compress(output.getData());
    }

    /**
     * The byte buffer with the column values.
     *
     * @return the byte buffer
     * @throws java.io.IOException
     */
    public byte[] getCompressedColumnValues() throws IOException {
        return compressedBuffer;
    }

    /**
     * The lowest value in this column.
     *
     * @return the lowest value in this column.
     */
    public Key getLowestValue() {
        return low;
    }

    /**
     * The highest value in this column.
     *
     * @return the highest value in this column.
     */
    public Key getHighestValue() {
        return high;
    }

    /**
     * True if this column is of type {@link Key}.
     *
     * @return True if this column is of type {@link Key}, false otherwise.
     */
    public boolean isKeyColumn() {
        return keyColumn;
    }

    /**
     * The Compressor used by this column.
     *
     * @return the compressor used by this column.
     */
    public ICompressor getCompressor() {
        return compressor;
    }

    /**
     * Updates the low / high value of the column.
     *
     * @param value the newly inserted value.
     */
    private void setLowHighValues(Value value) {
        if (keyColumn) {
            if (low == null || low.compareTo((Key) value) > 0) {
                Key k = (Key) Utils.copy(value);
                low = k;
            }
            if (high == null || high.compareTo((Key) value) < 0) {
                Key k = (Key) Utils.copy(value);
                high = k;
            }
        }
    }

    @Override
    public int compareTo(OutputColumn o) {
        if (this.columnPosition < o.columnPosition)
            return -1;
        if (this.columnPosition > o.columnPosition)
            return 1;
        return 0;
    }
}
