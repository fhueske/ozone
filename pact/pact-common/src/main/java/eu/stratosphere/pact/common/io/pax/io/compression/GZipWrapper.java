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

package eu.stratosphere.pact.common.io.pax.io.compression;

import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Compressor using the standard gzip library from java.
 * <p/>
 * {@link java.util.zip.Deflater}
 *
 */
public class GZipWrapper {

    public static class Compressor implements ICompressor {

        private final Deflater deflater = new Deflater();

        /**
         * byte buffer for the compressed data
         */
        private byte[] buffer;

        @Override
        public final byte[] compress(byte[] data) throws CompressorException {
            ensureCapacity(data.length);
            return deflate(data);
        }

        /**
         * Ensures the buffer has enough capacity to hold the compressed data.
         *
         * @param length the length of the uncompressed data
         */
        private void ensureCapacity(int length) {
            if (buffer == null) {
                buffer = new byte[length];
            } else {
                if (buffer.length < length) {
                    buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, length));
                }
            }
        }

        private byte[] deflate(byte[] data) {
            deflater.setInput(data);
            deflater.finish();
            int compressedLength = deflater.deflate(buffer);
            deflater.reset();
            return Arrays.copyOf(buffer, compressedLength);
        }

    }

    public static class Decompressor implements IDecompressor {

        private final Inflater inflater = new Inflater();

        @Override
        public void decompress(byte[] data, byte[] uncompressedBuffer) throws DecompressorException {
            inflater.setInput(data);
            try {
                inflater.inflate(uncompressedBuffer);
                inflater.reset();
            } catch (DataFormatException e) {
                throw new DecompressorException(e);
            }
        }
    }
}
