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

import java.util.ArrayList;
import java.util.List;

/**
 * Class to convert a selection into conjunctive normal form.
 * <p/>
 * After the conversion the negation in all predicates is pushed
 * down to their operators, making the predicates themself not negated
 * anymore.
 *
 */
public abstract class CNFConverter {

    /**
     * Returns the conjunctive normal form of the selection.
     *
     * @param selection the selection.
     * @return the conjunctive normal form of the selection.s
     */
    public static ISelection toCNF(ISelection selection) {

        return distributeANDoverOR(moveNOTInwards(selection));

    }

    /**
     * Pushes the negations as far down as possible.
     * At negated predicates the negation is pushed into their operators,
     * meaning a new predicate with the related negated operator is returned.
     * For instance !(<) -> >= or = -> !=
     *
     * @param selection the selection.
     * @return the selection without negated predicates.
     */
    public static ISelection moveNOTInwards(ISelection selection) {
        if (selection instanceof Composition) {
            Composition composition = (Composition) selection;
            if (composition.isNegated()) {
                List<ISelection> childrenOfAtom = new ArrayList<ISelection>(composition.getAtoms().size());
                for (ISelection child : composition.getAtoms()) {
                    childrenOfAtom.add(moveNOTInwards(child.negate()));
                }
                if (composition.getLogicalOperator() == LogicalOperator.AND) {
                    return promoteNested(composition, LogicalOperator.OR, childrenOfAtom);
                } else {
                    return promoteNested(composition, LogicalOperator.AND, childrenOfAtom);
                }
            } else {
                List<ISelection> children = new ArrayList<ISelection>(composition.getAtoms().size());
                for (ISelection atom : composition.getAtoms()) {
                    children.add(moveNOTInwards(atom));
                }
                return new Composition(composition.getLogicalOperator(), composition.getParent(), children);
            }
        } else { // Predicate
            Predicate predicate = (Predicate) selection;
            return predicate.isNegated() ? predicate.invertOperator() : predicate;
        }
    }

    private static ISelection distributeANDoverOR(ISelection selection) {
        if (selection instanceof Composition) {
            Composition composition = (Composition) selection;
            if (composition.getLogicalOperator() == LogicalOperator.OR) {
                Composition current = promoteNested(composition.getParent(), LogicalOperator.OR, composition.getAtoms());

                // check for conjunctions in children
                Composition conjunction = null;
                List<ISelection> others = new ArrayList<ISelection>();
                for (ISelection child : current.getAtoms()) {
                    if (conjunction == null && child instanceof Composition && ((Composition) child).getLogicalOperator() == LogicalOperator.AND) {
                        conjunction = (Composition) child;
                    } else {
                        others.add(child);
                    }
                }
                if (conjunction == null) {
                    return promoteNested(current.getParent(), LogicalOperator.OR, current.getAtoms());
                }

                ISelection rest;
                if (others.size() == 1) {
                    rest = others.get(0);
                } else {
                    rest = promoteNested(current.getParent(), LogicalOperator.OR, others);
                }

                List<ISelection> nested = new ArrayList<ISelection>(conjunction.getAtoms().size());
                for (ISelection item : conjunction.getAtoms()) {
                    Composition n = new Composition(LogicalOperator.OR, conjunction);
                    n.addAtom(rest);
                    n.addAtom(item);
                    nested.add(distributeANDoverOR(n));
                }
                return promoteNested(conjunction.getParent(), LogicalOperator.AND, nested);

            } else { //if (composition.getLogicalOperator() == LogicalOperators.AND) {
                List<ISelection> newAtoms = new ArrayList<ISelection>(composition.getAtoms().size());
                for (int i = 0; i < composition.getAtoms().size(); i++) {
                    newAtoms.add(distributeANDoverOR(composition.getAtoms().get(i)));
                }
                return promoteNested(composition.getParent(), LogicalOperator.AND, newAtoms);
            }
        } // atom
        else {
            return selection;
        }
    }

    private static Composition promoteNested(Composition parent, LogicalOperator operator, List<ISelection> atoms) {
        if (atoms.size() <= 1) {
            throw new RuntimeException();
        }

        List<ISelection> nested = new ArrayList<ISelection>();
        for (ISelection atom : atoms) {
            if (atom instanceof Composition && ((Composition) atom).getLogicalOperator() == operator) {
                nested.addAll(((Composition) atom).getAtoms());
            } else {
                nested.add(atom);
            }
        }
        return new Composition(operator, parent, nested);
    }

}
