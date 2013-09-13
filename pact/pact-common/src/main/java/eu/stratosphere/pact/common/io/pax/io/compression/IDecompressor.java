package eu.stratosphere.pact.common.io.pax.io.compression;

/**
 * Common interface for compression algorithms used
 * to decompress the columns of the row groups.
 *
 * @author Andreas Kunft
 */
public interface IDecompressor {

    /**
     * Decompress the data into the given buffer.
     *
     * @param data               the data to be decompressed.
     * @param uncompressedBuffer the buffer where the uncompressed data should be stored.
     */
    void decompress(byte[] data, byte[] uncompressedBuffer) throws DecompressorException;

    /**
     * Exceptions thrown by {@link IDecompressor} instances.
     */
    static class DecompressorException extends RuntimeException {

        public DecompressorException(String message) {
            super(message);
        }

        public DecompressorException(String message, Throwable cause) {
            super(message, cause);
        }

        public DecompressorException(Throwable cause) {
            super(cause);
        }
    }
}
