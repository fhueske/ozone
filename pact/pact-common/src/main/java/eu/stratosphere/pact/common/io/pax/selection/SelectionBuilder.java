package eu.stratosphere.pact.common.io.pax.selection;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.type.Key;

/**
 * Builder class to create a selection tree.
 *
 * @author Andreas Kunft
 */
public class SelectionBuilder {

    private static final char START_ID = 70; // F

    private char predicateID = START_ID;

    /**
     * The root of the selection tree.
     */
    private Composition root;

    /**
     * The current compositions new elements are added.
     */
    private Composition current;

    private SelectionBuilder() {
    }

    /**
     * Initializes the build.
     *
     * @return an instance of {@link SelectionBuilder}.
     */
    public static SelectionBuilder create() {
        return new SelectionBuilder();
    }

    /**
     * Creates a single local predicate.
     *
     * @param op       the operator of the predicate.
     * @param column   the column of the predicate.
     * @param literals the literals of the predicate.
     * @return a single local predicate
     */
    public static ISelection buildSinglePredicate(LocalOperator op, int column, Key... literals) {
        return singlePredicate(START_ID, null, false, op, column, literals);
    }

    /**
     * Creates a single negated local predicate.
     *
     * @param op       the operator of the predicate.
     * @param column   the column of the predicate.
     * @param literals the literals of the predicate.
     * @return a single local predicate
     */
    public static ISelection buildSingleNegatedPredicate(LocalOperator op, int column, Key... literals) {
        return singlePredicate(START_ID, null, true, op, column, literals);
    }

    /**
     * Adds an predicate to the current composition.
     *
     * @param op       the operator of the predicate.
     * @param column   the column of the predicate.
     * @param literals the literals of the predicate
     * @return the selection builder
     */
    public SelectionBuilder predicate(LocalOperator op, int column, Key... literals) {
        if (root == null) {
            throw new IllegalStateException("Create startComposition first. For single predicates use buildSinglePredicate instead");
        }

        ISelection selection = singlePredicate(newPredicateID(), current, false, op, column, literals);
        current.addAtom(selection);

        return this;
    }

    /**
     * Adds an predicate to the current composition.
     *
     * @param op       the operator of the predicate.
     * @param column   the column of the predicate.
     * @param literals the literals of the predicate
     * @return the selection builder
     */
    public SelectionBuilder negatedPredicate(LocalOperator op, int column, Key... literals) {
        if (root == null) {
            throw new IllegalStateException("Create startComposition first. For single predicates use buildSinglePredicate instead");
        }
        ISelection selection = singlePredicate(newPredicateID(), current, true, op, column, literals);
        current.addAtom(selection);

        return this;
    }

    /**
     * Adds a new composition as atom to the current composition
     * or sets it as root.
     *
     * @param operator the operator of the composition.
     * @return the selection builder
     */
    public SelectionBuilder startComposition(LogicalOperator operator) {
        if (root == null) {
            root = new Composition(operator, null);
            current = root;
        } else {
            Composition tmp = new Composition(operator, current);
            current.addAtom(tmp);
            current = tmp;
        }
        return this;
    }

    /**
     * Adds a new composition as atom to the current composition
     * or sets it as root.
     *
     * @param operator the operator of the composition.
     * @return the selection builder
     */
    public SelectionBuilder startNegatedComposition(LogicalOperator operator) {
        if (root == null) {
            root = new Composition(true, operator, null, new ArrayList<ISelection>());
            current = root;
        } else {
            Composition tmp = new Composition(true, operator, current, new ArrayList<ISelection>());
            current.addAtom(tmp);
            current = tmp;
        }
        return this;
    }

    /**
     * Ends the current composition. Compositions added after this call are added to the parent of this composition.
     *
     * @return the selection builder
     */
    public SelectionBuilder endComposition() {
        if (current.getAtoms().size() < 2) {
            throw new IllegalStateException("Compositions need at least two predicates");
        }
        if (current.getParent() != null) {
            current = current.getParent();
        }
        return this;
    }

    /**
     * Adds the atoms to the current composition.
     *
     * @param predicates the predicates.
     * @return the selection builder
     */
    public SelectionBuilder predicates(List<ISelection> predicates) {
        if (root == null) {
            throw new IllegalStateException("Create startComposition first. For single predicates use buildSinglePredicate instead");
        }
        for (ISelection predicate : predicates) {
            current.addAtom(predicate);
        }
        return this;
    }

    private static ISelection singlePredicate(char startID, Composition parent, boolean negated, LocalOperator op, int column, Key[] literals) {
        if (op == LocalOperator.BETWEEN) {
            if (literals.length != 2) {
                throw new RuntimeException("Operator requires 2 literals.");
            }
            List<ISelection> atoms = new ArrayList<ISelection>(2);
            atoms.add(new Predicate(startID, negated, LocalOperator.GREATER_EQUAL_THEN, column, literals[0]));
            atoms.add(new Predicate((char) (startID + 1), negated, LocalOperator.LESS_EQUAL_THEN, column, literals[1]));

            return new Composition(false, LogicalOperator.AND, null, atoms);
        }
        if (literals.length != 1) {
            throw new RuntimeException("Operator requires 1 literals.");
        }
        return new Predicate(startID, negated, op, column, literals[0]);
    }

    /**
     * Returns the built selection.
     *
     * @return the built selection
     */
    public ISelection build() {
        return CNFConverter.toCNF(root);
    }


    private char newPredicateID() {
        return predicateID++;
    }
}
