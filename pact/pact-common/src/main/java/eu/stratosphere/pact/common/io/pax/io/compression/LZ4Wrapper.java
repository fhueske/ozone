package eu.stratosphere.pact.common.io.pax.io.compression;

import java.util.Arrays;

/**
 * Compression using the LZ4 compression algorithm.
 * <p/>
 * <a>https://code.google.com/p/lz4/</a>
 * <a>https://github.com/jpountz/lz4-java</a>
 *
 * @author Andreas Kunft
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
