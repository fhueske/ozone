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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * An un-synchronized version of the java.io.ByteArrayOutputStream.
 * <p/>
 * All methods which are synchronized in the base class
 * are overwritten and NOT synchronized anymore.
 *
 */
public class UnSyncByteArrayOutputStream extends ByteArrayOutputStream {

    /**
     * Saves the number of bytes written since the last call of
     * {@link de.tuberlin.pax.io.UnSyncByteArrayOutputStream#getLastValueBytesWritten()}
     */
    private int lastValueBytesWritten;

    /**
     * Initializes the stream.
     * <p/>
     * The buffer size is set to the default size.
     * {@link java.io.ByteArrayOutputStream#ByteArrayOutputStream()}
     */
    public UnSyncByteArrayOutputStream() {
        super();
    }

    /**
     * Initializes the stream.
     * <p/>
     * The buffer size is set to the given value.
     *
     * @param initialSize the initial size of the byte buffer.
     */
    public UnSyncByteArrayOutputStream(int initialSize) {
        super(initialSize);
    }

    @Override
    public void write(int b) {
        int newcount = count + 1;
        if (newcount > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        buf[count] = (byte) b;
        count = newcount;
        lastValueBytesWritten += 1;
    }

    @Override
    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newcount = count + len;
        if (newcount > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        System.arraycopy(b, off, buf, count, len);
        count = newcount;
        lastValueBytesWritten += len;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, count);
    }

    @Override
    public void reset() {
        count = 0;
    }

    @Override
    public byte toByteArray()[] {
        return Arrays.copyOf(buf, count);
    }

    @Override
    public int size() {
        return count;
    }

    /**
     * Returns the bytes written since the last call
     * of this method.
     *
     * @return the bytes written since the last call of this method.
     */
    public int getLastValueBytesWritten() {
        int retval = lastValueBytesWritten;
        lastValueBytesWritten = 0;
        return retval;
    }

    @Override
    public String toString() {
        return new String(buf, 0, count);
    }

    @Override
    public String toString(String charsetName) throws UnsupportedEncodingException {
        return new String(buf, 0, count, charsetName);
    }

    @Override
    public void close() throws IOException {
    }

}