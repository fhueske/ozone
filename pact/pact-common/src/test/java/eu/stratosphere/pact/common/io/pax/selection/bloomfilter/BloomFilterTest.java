package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Andreas Kunft
 */
public class BloomFilterTest {

    BloomFilter bloomFilter;

    @Before
    public void before() {

    }

    @Test
    public void testCorrectness() {
        final int size = 10000;

        final double fpr = 0.1;

        BloomFilter bloomFilter = new BloomFilter(fpr, size);

        List<Integer> test = new ArrayList<Integer>();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int value = random.nextInt();
            test.add(value);
            bloomFilter.add(value + "");
        }

        int[] notIn = new int[size];
        for (int i = 0; i < size; ) {
            int value = random.nextInt();
            if (!test.contains(value)) {
                notIn[i++] = value;
            }
        }

        for (int i = 0; i < size; i++) {
            assertTrue(bloomFilter.contains(test.get(i) + ""));
        }

        int count = 0;
        for (int i = 0; i < size; i++) {
            if (bloomFilter.contains(notIn[i] + "")) {
                count++;
            }
        }
        System.out.println("FP (expected): " + (size * fpr) + ", (is): " + count);
    }
}
