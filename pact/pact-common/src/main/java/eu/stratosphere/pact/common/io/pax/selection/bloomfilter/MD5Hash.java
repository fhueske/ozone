package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A hash function using MD5 hashing.
 */
class MD5Hash implements HashFunction<String> {

    private final MessageDigest digest;

    private final int salt;

    MD5Hash(int salt) {
        try {
            this.digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // this should never happen!
            throw new AssertionError();
        }
        this.salt = salt;
    }

    @Override
    public int asInt(String value) {
        digest.update((byte) salt);
        byte[] md5 = digest.digest(value.getBytes());
        digest.reset();
        return createInt(md5);
    }

    /**
     * Creates a 4 byte hash of the 16 byte md5 hash.
     *
     * @param md5 the 16 byte md5 hash value.
     * @return the xor'd 4 byte value
     */
    private int createInt(byte[] md5) {
        byte[] buffer = new byte[4];
        for (int i = 0; i < 4; i++) {
            buffer[0] ^= md5[i * 4];
            buffer[1] ^= md5[(i * 4) + 1];
            buffer[2] ^= md5[(i * 4) + 2];
            buffer[3] ^= md5[(i * 4) + 3];
        }

        int re = ((buffer[0]) << 24) |
                ((buffer[1] & 0xff) << 16) |
                ((buffer[2] & 0xff) << 8) |
                ((buffer[3] & 0xff));

        return Math.abs(re);

    }
}
