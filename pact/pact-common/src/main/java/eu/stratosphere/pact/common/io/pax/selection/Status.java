package eu.stratosphere.pact.common.io.pax.selection;

/**
 * Return value of the selection elements visited.
 *
 * @author Andreas Kunft
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
