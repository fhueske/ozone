package eu.stratosphere.pact.common.io.pax.io.compression;

import java.io.IOException;

import eu.stratosphere.pact.common.io.pax.io.UnSyncByteArrayInputStream;
import eu.stratosphere.pact.common.io.pax.io.UnSyncByteArrayOutputStream;

/**
 * Compression using the LZMA compression algorithm.
 * <p/>
 * As the reference implementation form 7zip is no longer
 * under development, the streaming mode library from
 * <a>https://github.com/jponge/lzma-java</a> is used.
 *
 * @author Andreas Kunft
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
            try {
                LzmaInputStream decompressor = new LzmaInputStream(buffer, new Decoder());
                if (decompressor.read(uncompressedData) != uncompressedData.length) {
                    throw new DecompressorException("Could not decompress the complete column");
                }
            } catch (IOException e) {
                throw new DecompressorException(e);
            }
        }
    }

}
