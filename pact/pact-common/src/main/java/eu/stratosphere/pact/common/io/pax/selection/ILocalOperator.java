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

import eu.stratosphere.pact.common.type.Key;

/**
 * Interface for the concrete implementations of local operators.
 *
 * @param <T>
 */
public interface ILocalOperator<T extends Key> {

    /**
     * Evaluates the column value against the literal of the local predicate.
     *
     * @param columnValue    the column value.
     * @param conditionValue the literal of the local predicate.
     * @return true if the predicate matched, false otherwise.
     */
    boolean evaluate(T columnValue, T conditionValue);

    /**
     * Evaluates the low / high values of column against the local predicate.
     * A return value of true indicates that an "real" evaluation might match.
     * A return value of false indicates that the evaluation of the predicate
     * will NOT match for the whole row group.
     *
     * @param low            the lowest value of the column.
     * @param high           the highest value of the column.
     * @param conditionValue the literal of the local predicate.
     * @return true if the the literals are in range, false otherwise.
     */
    boolean evaluateHighLow(T low, T high, T conditionValue);
}
