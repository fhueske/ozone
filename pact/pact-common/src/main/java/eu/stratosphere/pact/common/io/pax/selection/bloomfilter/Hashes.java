package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

/**
 * Available hash algorithms for the bloom filter.
 * <p/>
 * As the ordinal number is used to identify the hash in binary from,
 * do not alter their appericence. New hash factories must be appended
 * at the end to ensure correct behaviour with previously generated
 * files.
 *
 * @author Andreas Kunft
 */
public enum Hashes implements BloomFilter.Factory {

    MURMUR() {
        @Override
        public HashFunction<String> get(int seed) {
            return new MurmurHash(seed);
        }
    },

    BERNSTEIN() {
        @Override
        public HashFunction<String> get(int seed) {
            return new BernsteinHash(seed);
        }
    },

    MD5() {
        @Override
        public HashFunction<String> get(int seed) {
            return new MD5Hash(seed);
        }
    };
}
