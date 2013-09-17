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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.junit.Test;

import eu.stratosphere.pact.common.type.base.PactInteger;

public class CNFConverterTest {

    @Test
    public void testSinglePredicate() {
        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(0));

        String expected = "F";
        ISelection cnf = CNFConverter.toCNF(selection);

        assertTrue(cnf instanceof Predicate);
        assertEquals(expected, cnf.id());
        assertEquals(LocalOperator.EQUAL, ((Predicate) cnf).operator);
    }

    @Test
    public void testSingleNegatedPredicate() {
        ISelection selectionNegated = SelectionBuilder.buildSingleNegatedPredicate(LocalOperator.EQUAL, 0, new PactInteger(0));

        String expected = "F";
        ISelection cnfNegated = CNFConverter.toCNF(selectionNegated);

        assertTrue(cnfNegated instanceof Predicate);
        assertEquals(expected, cnfNegated.id());
        assertEquals(LocalOperator.NOT_EQUAL, ((Predicate) cnfNegated).operator);
    }

    @Test
    public void testSingleBetween() {
        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.BETWEEN, 0, new PactInteger(0), new PactInteger(1));
        ISelection selectionNegated = SelectionBuilder.buildSingleNegatedPredicate(LocalOperator.BETWEEN, 0, new PactInteger(0), new PactInteger(1));

        String expected = "(F&&G)";
        ISelection cnf = CNFConverter.toCNF(selection);
        ISelection cnfNegated = CNFConverter.toCNF(selectionNegated);

        assertTrue(cnf instanceof Composition);
        assertTrue(cnfNegated instanceof Composition);

        assertEquals(expected, cnf.id());
        assertEquals(expected, cnfNegated.id());

        assertEquals(LocalOperator.GREATER_EQUAL_THAN, ((Predicate) ((Composition) cnf).getAtoms().get(0)).operator);
        assertEquals(LocalOperator.LESS_EQUAL_THAN, ((Predicate) ((Composition) cnf).getAtoms().get(1)).operator);

        assertEquals(LocalOperator.LESS_THAN, ((Predicate) ((Composition) cnfNegated).getAtoms().get(0)).operator);
        assertEquals(LocalOperator.GREATER_THAN, ((Predicate) ((Composition) cnfNegated).getAtoms().get(1)).operator);
    }

    @Test
    public void testComplexSelection1() throws Exception {
        ISelection selection = SelectionBuilder.create()
                .startComposition(LogicalOperator.OR)
                .predicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .predicate(LocalOperator.EQUAL, 1, new PactInteger(1))
                .startComposition(LogicalOperator.AND)
                .predicate(LocalOperator.GREATER_THAN, 2, new PactInteger(1))
                .predicate(LocalOperator.EQUAL, 2, new PactInteger(2))
                .endComposition()
                .endComposition()
                .build();

        String expected = "(I||F||G)&&(F||G||H)";
        String cnf = CNFConverter.toCNF(selection).id();
        cnf = cnf.substring(1, cnf.length() - 1);

        assertTrue("expected: " + expected + ", is: " + cnf, checkEquality(expected, cnf));
    }

    @Test
    public void testComplexSelection2() throws Exception {
        // (P && ~Q) || (R && S) || (Q && R && ~S)
        ISelection selection = SelectionBuilder.create()
                .startComposition(LogicalOperator.OR)
                .startComposition(LogicalOperator.AND)
                .predicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .negatedPredicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .endComposition()
                .startComposition(LogicalOperator.AND)
                .predicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .predicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .endComposition()
                .startComposition(LogicalOperator.AND)
                .predicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .predicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .negatedPredicate(LocalOperator.EQUAL, 0, new PactInteger(0))
                .build();

        String expected = "(I||F||J)&&(I||F||K)&&(I||F||L)&&(I||G||J)&&(I||G||K)&&(I||G||L)&&(F||H||J)&&(F||H||K)&&(F||H||L)&&(G||H||J)&&(G||H||K)&&(G||H||L)";
        String cnf = CNFConverter.toCNF(selection).id();
        cnf = cnf.substring(1, cnf.length() - 1);

        assertTrue("expected: " + expected + ", is: " + cnf, checkEquality(expected, cnf));
    }

    @Test
    public void testNegation() throws Exception {
        // ~(F | G) ->  (~F & ~G)
        ISelection simpleOR = SelectionBuilder.create()
                .startNegatedComposition(LogicalOperator.OR)
                .predicate(LocalOperator.LESS_EQUAL_THAN, 0, new PactInteger(0))
                .predicate(LocalOperator.NOT_EQUAL, 1, new PactInteger(1))
                .endComposition()
                .build();
        String simpleORExpected = "(F&&G)";
        ISelection result = CNFConverter.moveNOTInwards(simpleOR);

        assertTrue("expected: " + simpleORExpected + ", is: " + result.id(), checkEquality(simpleORExpected, result.id()));
        assertTrue(result instanceof Composition);

        assertEquals(LocalOperator.GREATER_THAN, ((Predicate) ((Composition) result).getAtoms().get(0)).operator);
        assertEquals(LocalOperator.EQUAL, ((Predicate) ((Composition) result).getAtoms().get(1)).operator);

        // ~(F & G) ->(~F | ~G)
        ISelection simpleAND = SelectionBuilder.create()
                .startNegatedComposition(LogicalOperator.AND)
                .predicate(LocalOperator.LESS_EQUAL_THAN, 0, new PactInteger(0))
                .predicate(LocalOperator.NOT_EQUAL, 1, new PactInteger(1))
                .endComposition()
                .build();
        String simpleAndExpected = "(F||G)";
        result = CNFConverter.moveNOTInwards(simpleAND);

        assertTrue("expected: " + simpleAndExpected + ", is: " + result.id(), checkEquality(simpleAndExpected, result.id()));

        assertTrue(result instanceof Composition);

        assertEquals(LocalOperator.GREATER_THAN, ((Predicate) ((Composition) result).getAtoms().get(0)).operator);
        assertEquals(LocalOperator.EQUAL, ((Predicate) ((Composition) result).getAtoms().get(1)).operator);

        // ~(~(F | ~G) | H)) -> ((F | ~G) & ~H)
        ISelection complex = SelectionBuilder.create()
                .startNegatedComposition(LogicalOperator.OR)
                .startNegatedComposition(LogicalOperator.OR)
                .predicate(LocalOperator.GREATER_THAN, 0, new PactInteger(0))
                .negatedPredicate(LocalOperator.GREATER_EQUAL_THAN, 1, new PactInteger(1))
                .endComposition()
                .predicate(LocalOperator.LESS_EQUAL_THAN, 0, new PactInteger(0))
                .endComposition()
                .build();
        String complexExpected = "((F||G)&&H)";

        result = CNFConverter.moveNOTInwards(complex);
        assertTrue("expected: " + complexExpected + ", is: " + result.id(), checkEquality(complexExpected, result.id()));

        assertTrue(result instanceof Composition);

        assertEquals(LocalOperator.GREATER_THAN, ((Predicate) ((Composition) ((Composition) result).getAtoms().get(0)).getAtoms().get(0)).operator);
        assertEquals(LocalOperator.LESS_THAN, ((Predicate) ((Composition) ((Composition) result).getAtoms().get(0)).getAtoms().get(1)).operator);
        assertEquals(LocalOperator.GREATER_THAN, ((Predicate) ((Composition) result).getAtoms().get(1)).operator);
    }

    // HELPER

    public static boolean checkEquality(String selectionA, String selectionB) {
        List<String> a = new ArrayList<String>(Arrays.asList(selectionA.split("&&")));
        List<String> b = new ArrayList<String>(Arrays.asList(selectionB.split("&&")));

        ListIterator<String> aItr = a.listIterator();
        while (aItr.hasNext()) {
            String aAtom = aItr.next();
            aAtom = aAtom.substring(1, aAtom.length() - 1);
            boolean found = false;
            ListIterator<String> bItr = b.listIterator();
            while (bItr.hasNext()) {
                String bAtom = bItr.next();
                bAtom = bAtom.substring(1, bAtom.length() - 1);
                if (isSameDisjunction(aAtom, bAtom)) {
                    bItr.remove();
                    found = true;
                    break;
                }
            }
            if (found) {
                aItr.remove();
            } else {
                return false;
            }
        }
        return a.isEmpty() && b.isEmpty();
    }

    public static boolean isSameDisjunction(String a, String b) {
        List<String> aAtoms = new ArrayList<String>(Arrays.asList(a.split("\\|\\|")));
        List<String> bAtoms = new ArrayList<String>(Arrays.asList(b.split("\\|\\|")));

        ListIterator<String> aItr = aAtoms.listIterator();
        while (aItr.hasNext()) {
            String atom = aItr.next();
            if (bAtoms.contains(atom)) {
                bAtoms.remove(atom);
                aItr.remove();
            }
        }

        return aAtoms.isEmpty() && bAtoms.isEmpty();
    }
}
