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