package eu.stratosphere.pact.common.io.pax.selection;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.DataFormatException;

import eu.stratosphere.pact.common.io.pax.IndexedPAXInputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Visitor used to evaluate the selection against a row group.
 * <p/>
 * The evaluation strategy of the predicate depends on the
 * features set in the row group.
 * <p/>
 * In some cases the evaluation can result in an mismatch of the
 * predicate for the whole row group and not only for a specific
 * column.
 *
 * @author Andreas Kunft
 */
public class SelectionVisitor {

    /**
     * Optional index structure created on demand.
     */
    private BSearch tree;

    /**
     * The record the fields are written to in case of an match.
     */
    private final PactRecord record;

    /**
     * Header file with the row group meta data.
     */
    private final IndexedPAXInputFormat.Header header;

    /**
     * The columns of the row group.
     */
    private final IndexedPAXInputFormat.Column[] columns;

    /**
     * All columns already set in the record during selection evaluation.
     * This is useful to only to determine the other column values
     * still needed in the record to return all column values required by the
     * projection.
     */
    private final Set<Integer> setColumns;

    /**
     * The row in the row group the visitor is currently at.
     */
    private int currentRow;

    /**
     * Indicates whether sorting was used during the evaluation.
     * This value is important to to further evaluate the row group even if
     * the current row is the last row in the row group, which would normally indicate
     * no match was found.
     */
    private boolean sortingUsed;

    private final HashSet<Predicate> checkedHLKeysAlready = new HashSet<Predicate>();

    private final HashSet<Predicate> checkedBloomFilterAlready = new HashSet<Predicate>();

    public SelectionVisitor(PactRecord record, IndexedPAXInputFormat.Header header, IndexedPAXInputFormat.Column[] columns, int currentRow) {
        this.record = record;
        this.header = header;
        this.columns = columns;
        this.currentRow = currentRow;
        this.setColumns = new HashSet<Integer>(record.getNumFields());
    }

    /**
     * The actual evaluation of the given predicate.
     * <p/>
     * Depending on the set features of the row group,
     * different evaluation strategies are used.
     *
     * @param predicate the predicate to be evaluated.
     * @return the result of the evaluation
     * @throws Exception
     */
    public Status visit(Predicate predicate) throws Exception {
        final int position = predicate.position;
        final IndexedPAXInputFormat.Column col = columns[position];

        // check low / high
        /*if (!checkedHLKeysAlready.contains(predicate)) {
            checkedHLKeysAlready.add(predicate);

            if (!predicate.evaluateLowHigh(col.low, col.high)) {
                return Status.NO_MATCH_GLOBAL;
            }
        }*/

        if (!checkedBloomFilterAlready.contains(predicate)) {
            checkedBloomFilterAlready.add(predicate);
            // check bloom filter
            if (predicate.operator == LocalOperator.EQUAL) {
                if (header.hasBloomFilter(position) && !header.bloomFilterColumns.get(position).contains(predicate.literal.toString())) {
                    return Status.NO_MATCH_GLOBAL;
                }
            }
        }

        // check sorted column

        if (predicate.isUseSorting() && predicate.operator != LocalOperator.NOT_EQUAL) {
            // create Tree at first access
            if (tree == null) {
                tree = new BSearch(header.getSortedRows(position), columns[position]);
                sortingUsed = true;
            }
            return evaluateSortedColumn(predicate);
        }


        // table scan
        col.sync(currentRow);
        Key colValue = (Key) col.nextValue();
        if (predicate.evaluate(colValue)) {
            record.setField(predicate.position, colValue);
            setColumns.add(predicate.position);
            return Status.MATCH;
        }
        return Status.NO_MATCH;
    }

    /**
     * If the predicate exploits sorting, the evaluation is done by and index search
     * instead of a table scan.
     *
     * @param predicate the predicate to be evaluated.
     * @return the result of the predicate.
     * @throws IOException
     * @throws DataFormatException
     */
    private Status evaluateSortedColumn(Predicate predicate) throws IOException, DataFormatException {

        final int row;
        switch (predicate.operator) {
            case EQUAL:
                row = tree.get(predicate.literal);
                break;
            case GREATER_EQUAL_THEN:
                row = tree.getOrHigher(predicate.literal);
                break;
            case GREATER_THEN:
                row = tree.getHigher(predicate.literal);
                break;
            case LESS_EQUAL_THEN:
                row = tree.getOrLower(predicate.literal);
                break;
            case LESS_THEN:
                row = tree.getLower(predicate.literal);
                break;
            default:
                row = -1;
        }
        if (row != -1) {
            currentRow = row;

            record.setField(predicate.position, tree.getCurrentValue());
            setColumns.add(predicate.position);

            return Status.MATCH;
        } else {
            // global mismatch
            return Status.NO_MATCH_GLOBAL;
        }
    }

    /**
     * Checks if this column was already set in the given pact record
     * during selection evaluation.
     *
     * @param position the position to be checked.
     * @return true if the position was already set, false otherwise.
     */
    public boolean columnAlreadySet(int position) {
        return setColumns.contains(position);
    }

    /**
     * Increments the row the visitor should evaluate.
     *
     * @return the next row the visitor evaluates.
     */
    public int incrementAndGetRow() {
        return ++currentRow;
    }

    /**
     * The row the visitor evaluates currently.
     *
     * @return the row the visitor evaluates currently.
     */
    public int getCurrentRow() {
        return currentRow;
    }

    /**
     * Resets the set columns.
     */
    public void resetSetColumns() {
        setColumns.clear();
    }

    /**
     * True if the visitor used sorting during evaluation.
     *
     * @return true if the visitor used sorting during the evaluation, false otherwise.
     */
    public boolean useSorting() {
        return sortingUsed;
    }
}
