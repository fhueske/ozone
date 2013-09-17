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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.pact.common.type.base.PactInteger;

public class LocalOperatorTest {

    public static final PactInteger INT_ZERO = new PactInteger(0);
    public static final PactInteger INT_TWO = new PactInteger(2);
    public static final PactInteger INT_TEN = new PactInteger(10);
    public static final PactInteger INT_FOURTEEN = new PactInteger(14);

    @Test
    public void testEqual() {
        LocalOperator rel = LocalOperator.EQUAL;
        assertFalse(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_FOURTEEN));
        assertFalse(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_ZERO));

        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_TWO));
        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_TWO));

        assertFalse(rel.condition.evaluate(INT_TWO, INT_TEN));
        assertFalse(rel.condition.evaluate(INT_FOURTEEN, INT_ZERO));

        assertTrue(rel.condition.evaluate(INT_TEN, INT_TEN));
        assertTrue(rel.condition.evaluate(INT_ZERO, INT_ZERO));
    }

    @Test
    public void testNotEqual() {
        LocalOperator rel = LocalOperator.NOT_EQUAL;
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_ZERO));

        assertFalse(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_TWO));
        assertFalse(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_FOURTEEN));
        assertFalse(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_TWO));

        assertTrue(rel.condition.evaluate(INT_TWO, INT_TEN));
        assertTrue(rel.condition.evaluate(INT_FOURTEEN, INT_ZERO));

        assertFalse(rel.condition.evaluate(INT_TEN, INT_TEN));
        assertFalse(rel.condition.evaluate(INT_ZERO, INT_ZERO));
    }

    @Test
    public void testGreaterThen() {
        LocalOperator rel = LocalOperator.GREATER_THAN;
        assertFalse(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_ZERO));

        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_TWO));
        assertFalse(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_TWO));

        assertFalse(rel.condition.evaluate(INT_TWO, INT_TEN));
        assertTrue(rel.condition.evaluate(INT_FOURTEEN, INT_ZERO));

        assertFalse(rel.condition.evaluate(INT_TEN, INT_TEN));
        assertFalse(rel.condition.evaluate(INT_ZERO, INT_ZERO));
    }

    @Test
    public void testGreaterEqualThen() {
        LocalOperator rel = LocalOperator.GREATER_EQUAL_THAN;
        assertFalse(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_ZERO));

        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_TWO));
        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_TWO));

        assertFalse(rel.condition.evaluate(INT_TWO, INT_TEN));
        assertTrue(rel.condition.evaluate(INT_FOURTEEN, INT_ZERO));

        assertTrue(rel.condition.evaluate(INT_TEN, INT_TEN));
        assertTrue(rel.condition.evaluate(INT_ZERO, INT_ZERO));
    }

    @Test
    public void testLessThen() {
        LocalOperator rel = LocalOperator.LESS_THAN;
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_FOURTEEN));
        assertFalse(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_ZERO));

        assertFalse(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_TWO));
        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_TWO));

        assertTrue(rel.condition.evaluate(INT_TWO, INT_TEN));
        assertFalse(rel.condition.evaluate(INT_FOURTEEN, INT_ZERO));

        assertFalse(rel.condition.evaluate(INT_TEN, INT_TEN));
        assertFalse(rel.condition.evaluate(INT_ZERO, INT_ZERO));
    }

    @Test
    public void testLessEqualThen() {
        LocalOperator rel = LocalOperator.LESS_EQUAL_THAN;
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_FOURTEEN));
        assertFalse(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_ZERO));

        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_TWO));
        assertTrue(rel.condition.evaluateHighLow(INT_TWO, INT_FOURTEEN, INT_FOURTEEN));
        assertTrue(rel.condition.evaluateHighLow(INT_ZERO, INT_TEN, INT_TWO));

        assertTrue(rel.condition.evaluate(INT_TWO, INT_TEN));
        assertFalse(rel.condition.evaluate(INT_FOURTEEN, INT_ZERO));

        assertTrue(rel.condition.evaluate(INT_TEN, INT_TEN));
        assertTrue(rel.condition.evaluate(INT_ZERO, INT_ZERO));
    }
}
