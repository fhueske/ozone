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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * Compression using the LZ4 compression algorithm.
 * <p/>
 * <a>https://code.google.com/p/lz4/</a>
 * <a>https://github.com/jpountz/lz4-java</a>
 *
 */
public class LZ4Wrapper {

    public static final LZ4Factory FACTORY = LZ4Factory.fastestJavaInstance();

    public static class Compressor implements ICompressor {

        private final LZ4Compressor compressor = LZ4Wrapper.FACTORY.fastCompressor();

        @Override
        public byte[] compress(byte[] data) {
            byte[] out = new byte[data.length];
            final int length = compressor.compress(data, 0, data.length, out, 0);
            return Arrays.copyOf(out, length);
        }
    }

    public static class Decompressor implements IDecompressor {

        private final LZ4Decompressor decompressor = LZ4Wrapper.FACTORY.decompressor();

        @Override
        public void decompress(byte[] data, byte[] uncompressedBuffer) throws DecompressorException {
            decompressor.decompress(data, 0, uncompressedBuffer, 0, uncompressedBuffer.length);
        }
    }
}
