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

package eu.stratosphere.pact.common.io.pax.io;

import java.io.DataOutputStream;

/**
 * DataOutputStream which writes to an Byte Buffer and is NOT synchronized.
 * <p/>
 * All methods which are synchronized in the base class
 * are overwritten and NOT synchronized anymore.
 *
 */
public class ByteDataOutputStream extends DataOutputStream {

    private final UnSyncByteArrayOutputStream buffer;

    public ByteDataOutputStream() {
        this(new UnSyncByteArrayOutputStream());
    }

    /**
     * Creates a new DataOutputStream which writes internal to a
     * byte array via a {@link UnSyncByteArrayOutputStream}.
     * <p/>
     * The initial size of the byte array is set to the specified value.
     *
     * @param initialSize the initial size of the byte array the stream writes to.
     */
    public ByteDataOutputStream(int initialSize) {
        this(new UnSyncByteArrayOutputStream(initialSize));
    }

    private ByteDataOutputStream(UnSyncByteArrayOutputStream out) {
        super(out);
        buffer = out;
    }

    /**
     * Returns the byte array containing the written values.
     *
     * @return the byte array containing the written values.
     */
    public byte[] getData() {
        return buffer.toByteArray();
    }

    public void reset() {
        written = 0;
        buffer.reset();
    }

    @Override
    public void write(int b) {
        buffer.write(b);
        incCount(1);
    }

    @Override
    public void write(byte[] b) {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        buffer.write(b, off, len);
        incCount(len);
    }

    private void incCount(int value) {
        if (written + value < 0) {
            written = Integer.MAX_VALUE;
        } else {
            written += value;
        }
    }

    /**
     * Returns the number of bytes written to the stream since the last call of this method.
     *
     * @return the number of bytes written.
     */
    public int getLastBytesWritten() {
        return buffer.getLastValueBytesWritten();
    }
}
