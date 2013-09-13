package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import eu.stratosphere.pact.common.io.pax.Utils;

/**
 * A bloom filter implementation.
 * <p/>
 * Uses extended double hashing as proposed by
 * Adam Kirsch and Michael Mitzenmacher:
 * Less Hashing, Same Performance: Building a Better Bloom Filter
 * <p/>
 * of form h_1(v) + i * h_2(v) + f(i) with f(i) = i^3.
 * <p/>
 * Double hashing uses only 2 hash functions instead of k different hash functions
 * to map the value to the bloom filter without any loss in the asymptotic false positive probability.
 * <p/>
 * Default hashing applied is a modified version of Murmur3 Hashing.
 *
 * @author Andreas Kunft
 * @see MurmurHash
 */
public class BloomFilter {

    interface Factory {
        HashFunction<String> get(int seed);
    }

    /**
     * Bits used by an byte.
     */
    private final static int BITS = 8;

    /**
     * Mask for inserting the value into the correct position in the byte.
     */
    private final static char[] bitMask = new char[]{
            0x01,
            0x02,
            0x04,
            0x08,
            0x10,
            0x20,
            0x40,
            0x80
    };

    private static final int HASH_1_SEED = 0x85ebca6b;

    private static final int HASH_2_SEED = 0xc2b2ae35;

    /**
     * The array containing the hashed values.
     */
    private final byte[] bitSet;

    /**
     * the number of hash functions used.
     */
    private final int k;

    /**
     * the lengths of the filter in bits.
     */
    private final int m;

    private final HashFunction<String> hash_1;

    private final HashFunction<String> hash_2;

    private final int hashID;

    /**
     * Creates an bloom filter instance out of an previously written
     * filter (through the {@link BloomFilter#write(java.io.OutputStream)} method).
     *
     * @param stream the input stream
     * @throws IOException
     */
    public BloomFilter(InputStream stream) throws IOException {
        this.m = Utils.readInt(stream);
        this.k = Utils.readInt(stream);

        this.hashID = Utils.readInt(stream);

        Factory factory = Hashes.values()[hashID];
        hash_1 = factory.get(HASH_1_SEED);
        hash_2 = factory.get(HASH_2_SEED);

        this.bitSet = new byte[m / BITS];
        stream.read(this.bitSet);
    }

    public BloomFilter(double fpp, int expectedValues, Factory factory) {
        // optimized version to calculate m as expectedValues and errorRate have to be positive
        int tmpM = (int) (-expectedValues * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        this.m = tmpM + ((tmpM % BITS != 0) ? BITS - (tmpM % BITS) : 0);

        this.k = Math.max(1, (int) Math.round(Math.log(2) * (this.m / expectedValues)));

        this.bitSet = new byte[m / BITS];

        // TODO: refactor complete mechanism into strategy so we just hold the fac, which has set and contain

        // save to cast as only hashes can only be implemented in bloom filter
        this.hashID = ((Hashes) factory).ordinal();

        hash_1 = factory.get(HASH_1_SEED);
        hash_2 = factory.get(HASH_2_SEED);
    }

    public BloomFilter(double fpp, int expectedValues) {
        this(fpp, expectedValues, Hashes.MURMUR);
    }

    /**
     * Writes the bloom filter to the specified stream.
     *
     * @param stream the stream the bloom filter is written to.
     */
    public void write(OutputStream stream) throws IOException {
        Utils.writeInt(stream, m);
        Utils.writeInt(stream, k);
        Utils.writeInt(stream, hashID);
        stream.write(bitSet);
    }

    /**
     * Adds an value to the bloom filter.
     *
     * @param value to be added to the bloom filter.
     */
    public void add(String value) {
        int hash1 = hash_1.asInt(value);
        int hash2 = hash_2.asInt(value);
        for (int i = 1; i <= k; i++) {
            int nextHash = hash1 + i * hash2 + (i * i * i);
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            nextHash = nextHash % m;
            bitSet[nextHash / BITS] |= bitMask[nextHash % BITS];
        }
    }

    /**
     * Checks whether the value is contained in the bloom filter or not.
     * <p/>
     * Keep in mind that bloom filters are false positive, which means that even
     * if this method returns true there is a possibility that the value is actually not contained.
     *
     * @param value the value to be checked.
     * @return true if the value is in the bloom filter, false otherwise.
     */
    public boolean contains(String value) {
        int hash1 = hash_1.asInt(value);
        int hash2 = hash_2.asInt(value);
        for (int i = 1; i <= k; i++) {
            // extended double hashing h_1 + i * h_2 + f(i)
            int nextHash = hash1 + i * hash2 + (i * i * i);
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            nextHash = nextHash % m;
            if ((bitSet[nextHash / BITS] & bitMask[nextHash % BITS]) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the byte array of the filter.
     *
     * @return the byte array of the filter.
     */
    public byte[] getBitSet() {
        return bitSet;
    }

    /**
     * Returns the size of the bloom filter in bytes.
     *
     * @return the size of the bloom filter in bytes.
     */
    public int getSizeOnDisk() {
        return (m / BITS) +   // bitset
                4 +  // m
                4 +  // k
                4;   // hash ordinal
    }

    /**
     * Deletes all saved values in the bloom filter.
     */
    public void reset() {
        Arrays.fill(bitSet, (byte) 0);
    }

    public int getK() {
        return k;
    }

    public int getM() {
        return m;
    }

}
