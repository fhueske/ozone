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
 * Common interface for the elements of the selection tree.
 * <p/>
 * A selection is a tree of compositions as inner nodes and local predicates as leafs.
 *
 */
public interface ISelection {

    /**
     * Evaluates the visitor for the element in the selection tree.
     *
     * @param visitor the visitor which evaluates the selection tree.
     * @return true, if the selection matched, false otherwise.
     * @throws Exception
     */
    Status accept(SelectionVisitor visitor) throws Exception;

    /**
     * Negates the selection.
     * This means if the selection was not negated before it is negated afterwards
     * and is not negated anymore if it was before.
     *
     * @return the negated selection
     */
    ISelection negate();

    /**
     * Is the selection negated.
     *
     * @return true if the selection is negated, false otherwise.
     */
    boolean isNegated();

    /**
     * The runtime unique id of the selection.
     * <p/>
     * This method is mostly for test purposes, to ease the correctness evaluation.
     *
     * @return a runtime unique string representation of the selection.
     */
    String id();
}
