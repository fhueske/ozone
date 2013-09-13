package eu.stratosphere.pact.common.io.pax.selection;

/**
 * Supported logical operators for the composition.
 *
 * @author Andreas Kunft
 */
public enum LogicalOperator {

    /**
     * Conjunction
     */
    AND,

    /**
     * Disjunction
     */
    OR;

    @Override
    public String toString() {
        switch (this) {
            case AND:
                return "&&";
            case OR:
                return "||";
            default:
                return "?";
        }
    }


}
