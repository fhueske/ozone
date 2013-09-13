package eu.stratosphere.pact.common.io.pax.io;

import java.io.DataOutputStream;

/**
 * DataOutputStream which writes to an Byte Buffer and is NOT synchronized.
 * <p/>
 * All methods which are synchronized in the base class
 * are overwritten and NOT synchronized anymore.
 *
 * @author Andreas Kunft
 */
public class ByteDataOutputStream extends DataOutputStream {

    private final UnSyncByteArrayOutputStream buffer;

    public ByteDataOutputStream() {
        this(new UnSyncByteArrayOutputStream());
    }

    /**
     * Creates a new DataOutputStream which writes internal to a
     * byte array via a {@link UnSyncByteArrayOutputStream}.
     * <p/>
     * The initial size of the byte array is set to the specified value.
     *
     * @param initialSize the initial size of the byte array the stream writes to.
     */
    public ByteDataOutputStream(int initialSize) {
        this(new UnSyncByteArrayOutputStream(initialSize));
    }

    private ByteDataOutputStream(UnSyncByteArrayOutputStream out) {
        super(out);
        buffer = out;
    }

    /**
     * Returns the byte array containing the written values.
     *
     * @return the byte array containing the written values.
     */
    public byte[] getData() {
        return buffer.toByteArray();
    }

    public void reset() {
        written = 0;
        buffer.reset();
    }

    @Override
    public void write(int b) {
        buffer.write(b);
        incCount(1);
    }

    @Override
    public void write(byte[] b) {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        buffer.write(b, off, len);
        incCount(len);
    }

    private void incCount(int value) {
        if (written + value < 0) {
            written = Integer.MAX_VALUE;
        } else {
            written += value;
        }
    }

    /**
     * Returns the number of bytes written to the stream since the last call of this method.
     *
     * @return the number of bytes written.
     */
    public int getLastBytesWritten() {
        return buffer.getLastValueBytesWritten();
    }
}
