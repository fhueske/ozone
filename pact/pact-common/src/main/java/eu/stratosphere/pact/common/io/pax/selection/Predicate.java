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
 * A local predicate on a column in the row group.
 *
 */
public class Predicate implements ISelection {

    /**
     * The operator of this predicate.
     */
    public final LocalOperator operator;

    /**
     * The literals of this predicate.
     */
    public final Key literal;

    /**
     * The column position this predicate is evaluated against.
     */
    public final int position;

    /**
     * true if this predicate is negated, false otherwise.
     */
    private boolean negated;

    /**
     * A unique identifier (unique for the the executed job).
     */
    public final char id;

    /**
     * This value is set before the selection is evaluated.
     * It indicates (if set true) that this predicate is on a sorted column
     * AND the sorted indexes are used to evaluate this predicate.
     */
    private boolean useSorting;

    /**
     * Creates a new predicate.
     *
     * @param operator the operator of the predicate.
     * @param position the column position of this predicate.
     * @param literal  the literal of this predicate
     */
    Predicate(char id, LocalOperator operator, int position, Key literal) {
        this(id, false, operator, position, literal);
    }

    /**
     * Creates a new predicate.
     *
     * @param operator the operator of the predicate.
     * @param position the column position of this predicate.
     * @param literal  the literal of this predicate
     */
    Predicate(char id, boolean negated, LocalOperator operator, int position, Key literal) {
        this.id = id;
        this.negated = negated;
        this.operator = operator;
        this.position = position;
        this.literal = literal;
        this.useSorting = false;
    }

    /**
     * Evaluates the predicate against the specified column value.
     *
     * @param columnValue the value the predicate is evaluated against.
     * @return true, if the predicate matched, false otherwise.
     */
    public boolean evaluate(Key columnValue) {
        return operator.condition.evaluate(columnValue, literal);
    }

    /**
     * Evaluates the predicate against the specified low/high values of the column.
     * <p/>
     * This method returns possible false positives, which means even if this method
     * returns true, it does not mean that the predicate will match.
     * <p/>
     * On the other hand, if this method returns false, the predicate will not match
     * for any value in this column.
     *
     * @param low  the lowest value of the column.
     * @param high the highest value of the column.
     * @return true, if the predicate match or could match, false otherwise.
     */
    public boolean evaluateLowHigh(Key low, Key high) {
        return operator.condition.evaluateHighLow(low, high, literal);
    }

    @Override
    public Status accept(SelectionVisitor visitor) throws Exception {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return negated ? "~" : "" + "[c(" + position + ") " + operator + " " + literal.toString() + "]";
    }

    public String id() {
        return (negated) ? "~" + id : "" + id;
    }

    @Override
    public boolean isNegated() {
        return negated;
    }

    @Override
    public Predicate negate() {
        this.negated = !this.negated;
        return this;
    }

    /**
     * Returns a new instance of this predicate with negated operator.
     *
     * @return a new instance of this predicate with negated operator.
     */
    public Predicate invertOperator() {
        return new Predicate(this.id, !negated, operator.invert(), this.position, this.literal);
    }

    /**
     * Indicates that this predicate exploits sorting on the column.
     *
     * @return true if this predicate exploits sorting, false otherwise.
     */
    public boolean isUseSorting() {
        return useSorting;
    }

    /**
     * Sets whether this predicate should exploit sorting or not.
     *
     * @param useSorting true if this predicate should exploit sorting, false otherwise.
     */
    public void setUseSorting(boolean useSorting) {
        this.useSorting = useSorting;
    }
}
