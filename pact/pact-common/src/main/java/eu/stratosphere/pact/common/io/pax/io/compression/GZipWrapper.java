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
 * @author Andreas Kunft
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
