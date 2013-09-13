package eu.stratosphere.pact.common.io.pax.io;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * DataOutputStream which writes to an Byte Buffer and is NOT synchronized.
 * <p/>
 * The underlying byte buffer can be changed.
 * <p/>
 * IMPORTANT:
 * DO NOT USE THE skipBytes(int n) METHOD.
 * This lead to the usage of the skip method provided
 * by InputStream, which is not efficient for the
 * used UnSyncByteArrayInputStream.
 * <p/>
 * Use skip and rewind instead.
 *
 * @author Andreas Kunft
 */
public class ByteDataInputStream extends DataInputStream {

    private final UnSyncByteArrayInputStream buffer;

    /**
     * Creates a DataInputStream with the specified buffer as input.
     *
     * @param byteBuffer the byte array used as input.
     */
    public ByteDataInputStream(byte[] byteBuffer) {
        this(new UnSyncByteArrayInputStream(byteBuffer));
    }

    private ByteDataInputStream(UnSyncByteArrayInputStream in) {
        super(in);
        buffer = in;
    }

    /**
     * Set a new byte buffer to read from.
     *
     * @param byteBuffer the new byte buffer
     */
    public void changeBuffer(byte[] byteBuffer) throws IOException {
        buffer.setNewBuffer(byteBuffer);
        this.reset();
    }

    @Override
    public void reset() throws IOException {
        buffer.reset();
    }

    /**
     * Sets the position n bytes forward.
     *
     * @param n the number of bytes the position is moved forwards.
     * @return the actual number of bytes moved forwards.
     */
    public int skip(int n) {
        // important to use buffer here. Otherwise the InputStream skip method is used.
        return (int) buffer.skip(n);
    }

    /**
     * Sets the position n bytes backward.
     *
     * @param n the number of bytes the position is moved backwards.
     * @return the actual number of bytes moved backwards.
     */
    public int rewind(int n) {
        return (int) buffer.rewind(n);
    }
}
