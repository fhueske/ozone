package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

/**
 * A hash algorithm produced by Professor Daniel J. Bernstein.
 */
class BernsteinHash implements HashFunction<String> {

    private final int salt;

    BernsteinHash(int salt) {
        this.salt = salt;
    }

    @Override
    public int asInt(String value) {
        String hash = salt + value;
        return Math.abs((int) bernstein(hash));
    }

    long bernstein(String value) {
        long hash = 5381;

        for (int i = 0; i < value.length(); i++) {
            hash = ((hash << 5) + hash) + value.charAt(i);
        }

        return hash;
    }

}