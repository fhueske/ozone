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

/**
 * Simple Factory to create Implementations of
 * the {@link ICompressor} interface.
 *
 */
public class CompressionCodecs {

    /**
     * GZip compression instance.
     *
     * @return new GZip compression instance.
     */
    public static ICompressor getGZip() {
        return new GZipWrapper.Compressor();
    }

    /**
     * Lzma compression instance.
     *
     * @return new Lzma compression instance.
     */
    public static ICompressor getLzma() {
        return new LzmaWrapper.Compressor();
    }

    public static ICompressor getLZ4() {
        return new LZ4Wrapper.Compressor();
    }

    /**
     * Returns the associated byte value for the compressor instance.
     *
     * @param compressor the compressor instance.
     * @return the associated byte value for the compressor instance.
     */
    public static byte getCodecID(ICompressor compressor) {
        if (compressor instanceof GZipWrapper.Compressor) {
            return 0x00;
        }
        if (compressor instanceof LzmaWrapper.Compressor) {
            return 0x01;
        }
        if (compressor instanceof LZ4Wrapper.Compressor) {
            return 0x04;
        }

        throw new IllegalArgumentException("Add case for new compression class to this method");
    }

    /**
     * Creates the decompressor associated with the byte value.
     *
     * @param id the byte value of the decompressor.
     * @return the decompressor associated with the byte value.
     */
    public static IDecompressor getDecompressorForID(byte id) {
        switch (id) {
            case 0x00:
                return new GZipWrapper.Decompressor();
            case 0x01:
                return new LzmaWrapper.Decompressor();
            case 0x04:
                return new LZ4Wrapper.Decompressor();
        }

        throw new IllegalArgumentException("Add case for new compression class to this method.");
    }

}
