/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.io.pax.selection.bloomfilter;

import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

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
