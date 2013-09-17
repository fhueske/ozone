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

package eu.stratosphere.pact.common.io.pax.selection;

/**
 * Return value of the selection elements visited.
 *
 */
public enum Status {

    /**
     * Predicate / Composition matched for the current row.
     */
    MATCH,

    /**
     * Predicate / Composition did not match for the current row.
     */
    NO_MATCH,

    /**
     * Predicate / Composition did not match for the whole row group.
     * This happens in case of bloom filter or low/high value evaluation.
     */
    NO_MATCH_GLOBAL
}
