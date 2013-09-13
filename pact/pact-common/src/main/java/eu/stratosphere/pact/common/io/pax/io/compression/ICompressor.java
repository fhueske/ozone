package eu.stratosphere.pact.common.io.pax.io.compression;

/**
 * Common interface for compression algorithms used
 * to compress the columns of the row groups.
 *
 * @author Andreas Kunft
 */
public interface ICompressor {

    /**
     * Compresses the data.
     * <p/>
     * Continuous calls to this method must produce
     * separately compressed blocks of byte data.
     *
     * @param data the data to be compressed.
     * @return the compressed data
     */
    byte[] compress(byte[] data);

    /**
     * Exceptions thrown by {@link ICompressor} instances.
     */
    static class CompressorException extends RuntimeException {

        public CompressorException(String message) {
            super(message);
        }

        public CompressorException(String message, Throwable cause) {
            super(message, cause);
        }

        public CompressorException(Throwable cause) {
            super(cause);
        }
    }
}
