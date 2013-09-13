package eu.stratosphere.pact.common.io.pax.io.compression;

/**
 * Compression using the QuickLZ compression library.
 * <p/>
 * The java version of QuickLZ supports only block compression, no streaming
 * compression. Furthermore only compression levels 1 (fast compression) and
 * level 3 (fast decompression).
 * <p/>
 * The current implementation does not allow to reuse the byte buffer used to
 * compress the data.
 *
 * @author Andreas Kunft
 */
public class QuickLZWrapper {

    public static class Compressor implements ICompressor {

        private final static int COMPRESSION_LEVEL = 3;

        @Override
        public byte[] compress(byte[] data) throws CompressorException {
            return QuickLZ.compress(data, COMPRESSION_LEVEL);
        }

    }

    public static class Decompressor implements IDecompressor {

        @Override
        public void decompress(byte[] data, byte[] uncompressedData) throws DecompressorException {
            byte[] uncompressed = QuickLZ.decompress(data);
            if (uncompressed.length != uncompressedData.length) {
                throw new DecompressorException("Could not decompress the complete column");
            }
            System.arraycopy(uncompressed, 0, uncompressedData, 0, uncompressed.length);
        }
    }
}
