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

import java.io.ByteArrayInputStream;

/**
 * An un-synchronized version of the java.io.ByteArrayInputStream.
 * <p/>
 * All methods which are synchronized in the base class
 * are overwritten and NOT synchronized anymore.
 *
 */
public class UnSyncByteArrayInputStream extends ByteArrayInputStream {

    /**
     * Initialize the stream with the given buffer.
     *
     * @param bytes the buffer the stream reads from.
     */
    public UnSyncByteArrayInputStream(byte[] bytes) {
        super(bytes);
    }

    @Override
    public int read() {
        return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    @Override
    public int read(byte b[], int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (pos >= count) {
            return -1;
        }
        if (pos + len > count) {
            len = count - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    @Override
    public long skip(long n) {
        if (pos + n > count) {
            n = count - pos;
        }
        if (n < 0) {
            return 0;
        }
        pos += n;
        return n;
    }

    /**
     * Rewinds the position of the stream n bytes backwards.
     * <p/>
     * If the start of the stream is reached, it is not moved further backwards.
     *
     * @param n the number bytes to move backwards
     * @return the actual bytes moved backwards.
     */
    public long rewind(long n) {
        if (pos - n < 0 || n < 0) {
            n = pos;
        }
        pos -= n;
        return n;
    }

    @Override
    public int available() {
        return count - pos;
    }

    /**
     * Sets a new buffer the stream reads from.
     *
     * @param buffer the new buffer of the stream
     */
    public void setNewBuffer(byte[] buffer) {
        super.buf = buffer;
        super.mark = 0;
        super.count = buffer.length;
        super.pos = 0;
    }

    @Override
    public void reset() {
        super.pos = 0;
    }


}
