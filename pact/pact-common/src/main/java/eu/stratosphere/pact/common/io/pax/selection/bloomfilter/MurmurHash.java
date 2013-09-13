package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

/**
 * Murmur Hash Implementation based on:
 *
 * @see "http://smhasher.googlecode.com/svn/trunk/MurmurHash3.cpp"
 * @see "https://code.google.com/p/chromium/codesearch#chromium/src/third_party/leveldatabase/src/util/hash.cc"
 *      <p/>
 *      Modified to work in Java.
 */
class MurmurHash implements HashFunction<String> {

    private final static int m = 0xc6a4a793;
    private static final int r = 24;
    private final int seed;

    MurmurHash(int seed) {
        this.seed = seed;
    }

    @Override
    public int asInt(String value) {
        final int limit = value.length();
        int h = seed ^ (limit * m);

        int index = 0;
        while (index + 4 <= limit) {
            final int word = (value.charAt(index) & 0xFF) |
                    (value.charAt(index + 1) & 0xFF) << 8 |
                    (value.charAt(index + 2) & 0xFF) << 16 |
                    (value.charAt(index + 3) & 0xFF) << 24;
            index += 4;
            h += word;
            h *= m;
            h ^= (h >>> 16);
        }

        switch (limit - index) {
            case 3:
                h += (value.charAt(index + 2) & 0xFF) << 16;
            case 2:
                h += (value.charAt(index + 1) & 0xFF) << 8;
            case 1:
                h += (value.charAt(index) & 0xFF);
                h *= m;
                h ^= (h >>> r);
                break;
        }
        return h;
    }
}
