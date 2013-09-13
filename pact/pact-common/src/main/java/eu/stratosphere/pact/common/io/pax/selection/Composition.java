package eu.stratosphere.pact.common.io.pax.selection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A composition (Conjunction/Disjunction) containing
 * local predicates and/or compositions as atoms.
 * <p/>
 * The type of composition is defined by the {@link LogicalOperator} enum.
 *
 * @author Andreas Kunft
 */
public class Composition implements ISelection, Iterable<ISelection> {

    /**
     * The type of this composition.
     */
    private final LogicalOperator logicalOperator;

    /**
     * The atoms of the composition, which are either other compositions or local predicates.
     */
    private final List<ISelection> atoms;

    /**
     * The parent composition this composition is in.
     */
    private final Composition parent;

    private boolean negated;

    /**
     * Creates a new composition.
     *
     * @param logicalOperator the logical logicalOperator.
     * @param parent          the composition this composition resides in.
     * @param children        the atoms of the composition.
     */
    Composition(boolean negated, LogicalOperator logicalOperator, Composition parent, List<ISelection> children) {
        this.negated = negated;
        this.logicalOperator = logicalOperator;
        this.parent = parent;
        this.atoms = children;
    }

    /**
     * Creates a new composition.
     *
     * @param logicalOperator the logical logicalOperator.
     * @param parent          the composition this composition resides in.
     * @param children        the atoms of the composition.
     */
    Composition(LogicalOperator logicalOperator, Composition parent, List<ISelection> children) {
        this(false, logicalOperator, parent, children);
    }

    /**
     * Creates a new composition.
     *
     * @param logicalOperator the logical logicalOperator.
     * @param parent          the composition this composition resides in.
     */
    Composition(LogicalOperator logicalOperator, Composition parent) {
        this(logicalOperator, parent, new ArrayList<ISelection>());
    }

    /**
     * Returns the atoms of this composition.
     *
     * @return the atoms of this composition.
     */
    public List<ISelection> getAtoms() {
        return atoms;
    }

    /**
     * Adds a new atom.
     *
     * @param atom the atom.
     */
    void addAtom(ISelection atom) {
        this.atoms.add(atom);
    }

    /**
     * Returns the logical operator.
     *
     * @return the logical operator.
     */
    public LogicalOperator getLogicalOperator() {
        return logicalOperator;
    }

    /**
     * The parent of this composition.
     *
     * @return the parent of this composition, or null if its the root composition.
     */
    public Composition getParent() {
        return parent;
    }

    @Override
    public Iterator<ISelection> iterator() {
        return atoms.iterator();
    }

    /**
     * Runs the visitor on its atoms.
     * <p/>
     * Depending on the evaluation not all atoms are visited.
     * This method tries to return as fast as possible as the atoms don't match.
     *
     * @param visitor the visitor
     * @return true, if this composition matched, false otherwise.
     * @throws Exception
     */
    @Override
    public Status accept(SelectionVisitor visitor) throws Exception {
        Status match = Status.NO_MATCH;

        boolean orAllGlobal = true;
        for (ISelection atom : atoms) {
            match = atom.accept(visitor);
            // AND: first mismatch -> no match or global no match
            if (logicalOperator == LogicalOperator.AND) {
                if (match != Status.MATCH) {
                    return match;
                }
            }
            // OR:  first match -> true
            else if (logicalOperator == LogicalOperator.OR) {
                if (match == Status.MATCH) {
                    return match;
                } else if (match != Status.NO_MATCH_GLOBAL) {
                    orAllGlobal = false;
                }
            }
        }
        // AND: all matched -> MATCH
        // OR:  no atom matched -> NO_MATCH or NO_MATCH_GLOBAL
        if (logicalOperator == LogicalOperator.OR) {
            return orAllGlobal ? Status.NO_MATCH_GLOBAL : Status.NO_MATCH;
        }
        return match;
    }

    @Override
    public Composition negate() {
        this.negated = !negated;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = (negated) ? new StringBuilder("~(") : new StringBuilder("(");
        for (int i = 0; i < atoms.size(); i++) {
            builder.append(atoms.get(i));
            if (i < atoms.size() - 1) {
                builder.append(logicalOperator);
            }
        }
        return builder.append(")").toString();
    }

    @Override
    public String id() {
        StringBuilder builder = (negated) ? new StringBuilder("~(") : new StringBuilder("(");
        for (int i = 0; i < atoms.size(); i++) {
            builder.append(atoms.get(i).id());
            if (i < atoms.size() - 1) {
                builder.append(logicalOperator);
            }
        }
        return builder.append(")").toString();
    }

    public boolean isNegated() {
        return negated;
    }
}
