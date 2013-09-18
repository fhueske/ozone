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

/**
 * Available hash algorithms for the bloom filter.
 * <p/>
 * As the ordinal number is used to identify the hash in binary from,
 * do not alter their appericence. New hash factories must be appended
 * at the end to ensure correct behaviour with previously generated
 * files.
 *
 */
public enum Hashes implements BloomFilter.Factory {

    MURMUR() {
        @Override
        public HashFunction<String> get(int seed) {
            return new MurmurHash(seed);
        }
    };
}
