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

import java.io.IOException;

import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;
import lzma.streams.LzmaOutputStream;
import eu.stratosphere.pact.common.io.pax.io.UnSyncByteArrayInputStream;
import eu.stratosphere.pact.common.io.pax.io.UnSyncByteArrayOutputStream;

/**
 * Compression using the LZMA compression algorithm.
 * <p/>
 * As the reference implementation form 7zip is no longer
 * under development, the streaming mode library from
 * <a>https://github.com/jponge/lzma-java</a> is used.
 *
 */
public class LzmaWrapper {

    public static class Compressor implements ICompressor {

        private final UnSyncByteArrayOutputStream buffer;

        Compressor() {
            buffer = new UnSyncByteArrayOutputStream();
        }

        @Override
        public byte[] compress(byte[] data) {
            try {
                buffer.reset();

                LzmaOutputStream compressor = new LzmaOutputStream.Builder(buffer)
                        // Medium is (1 << 15), Max (1 << 28), default (1 << 23)
                        .useMediumDictionarySize()
                        .useMediumFastBytes()
                        .build();

                compressor.write(data);
                compressor.close();

                return buffer.toByteArray();
            } catch (IOException e) {
                throw new CompressorException(e);
            }
        }

    }

    public static class Decompressor implements IDecompressor {

        private UnSyncByteArrayInputStream buffer;

        @Override
        public void decompress(byte[] data, byte[] uncompressedData) throws DecompressorException {
            if (buffer == null) {
                this.buffer = new UnSyncByteArrayInputStream(data);
            } else {
                this.buffer.setNewBuffer(data);
            }
            LzmaInputStream decompressor = null;
            try {
                decompressor = new LzmaInputStream(buffer, new Decoder());
                if (decompressor.read(uncompressedData) != uncompressedData.length) {
                    throw new DecompressorException("Could not decompress the complete column");
                }
            } catch (IOException e) {
                throw new DecompressorException(e);
            } finally {
            	if(decompressor != null)
            		try{
            			decompressor.close();
            		} catch (IOException ioe) {
            			throw new DecompressorException("Could not close decompressor.");
            		}
            }
        }
    }

}
