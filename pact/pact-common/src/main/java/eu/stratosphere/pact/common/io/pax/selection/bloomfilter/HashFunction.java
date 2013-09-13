package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

/**
 * Common interface for hash functions used by the bloom filter.
 *
 * @param <T> the class of the values which are hashed.
 */
interface HashFunction<T> {

    /**
     * Returns the hash value.
     *
     * @param value the value to be hashed.
     * @return the hash value.
     */
    int asInt(T value);
}