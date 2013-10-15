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

import java.io.IOException;
import java.util.zip.DataFormatException;

import eu.stratosphere.pact.common.io.pax.IndexedPAXInputFormat;
import eu.stratosphere.pact.common.type.Key;

/**
 * Class for a binary search on the sorted rows of a column.
 * <p/>
 * The class expects a the binary sorted rows as specified by
 * bsearch.eps in the resources folder.
 */
public class BSearch {

    // STATIC DATA

    // the sortedRows sorted by the key in ascending order
    private final byte[] sortedRows;

    // the number of unique values and/or offsets
    private final int uniqueRowsLength;

    // the index in the sortedRows array where the duplicates start
    private final int duplicatesOffset;

    // the column with the actual values associated with sorted sortedRows
    private final IndexedPAXInputFormat.Column keys;

    // SEARCH SPECIFIC DATA

    // the current materialized value
    private Key currentValue;

    // the current row specifying the index of a value in the keys column
    private int currentRow = -1;

    // the current index in the sortedRows array
    private int currentIndex = -1;

    // DUPLICATE MANAGEMENT

    // if the value hit has duplicates, this index specifies the duplicates are located in the sortedRows array
    private int currentDuplicateIndex = -1;

    // if the value hit has duplicates, this number specifies how many duplicates are left
    private int numberOfDuplicatesLeft;

    /**
     * Creates the structure for the supplied sortedRows and keys.
     *
     * @param sortedRows the sorted sortedRows for the keys.
     * @param keys the keys for the sorted sortedRows
     */
    public BSearch(byte[] sortedRows, IndexedPAXInputFormat.Column keys) {
        this.sortedRows = sortedRows;
        this.keys = keys;
        this.uniqueRowsLength = (((sortedRows[0] & 0xff) << 24) + ((sortedRows[1] & 0xff) << 16) + ((sortedRows[2] & 0xff) << 8) + (sortedRows[3] & 0xff));

        this.duplicatesOffset = uniqueRowsLength + 1;
    }

    /**
     * The last value retrieved from the column.
     *
     * @return the last value retrieved from the column.
     */
    public Key getCurrentValue() {
        return currentValue;
    }

    /**
     * Returns the row for keys that are higher then the search key or -1 if there is no match.
     * <p/>
     * Consecutive calls to the method return the next matching keys.
     *
     * @param key the search key.
     * @return the matching row or -1.
     * @throws IOException
     * @throws DataFormatException
     */
    public int getHigher(Key key) throws IOException, DataFormatException {

        // matched before
        if (moreDuplicates()) return currentRow;
        if (currentIndex > 0) return moreLargerKeys();


        // first access to index, do bsearch
        currentIndex = search(key);
        if (currentIndex < 0) {
            currentIndex = Math.abs(currentIndex);
            // all smaller
            if (currentIndex >= duplicatesOffset) {
                return -1;
            }

        } else {
            // match get next higher
            currentIndex++;
            if (currentIndex >= duplicatesOffset) {
                return -1;
            }
        }
        // match or between
        syncAndGet(currentIndex);
        return currentRow;
    }

    /**
     * Returns the row for keys that are equal or higher then the search key or -1 if there is no match.
     * <p/>
     * Consecutive calls to the method return the next matching keys.
     *
     * @param key the search key.
     * @return the matching row or -1.
     * @throws IOException
     * @throws DataFormatException
     */
    public int getOrHigher(Key key) throws IOException, DataFormatException {
        // matched before
        if (moreDuplicates()) return currentRow;
        if (currentIndex > 0) return moreLargerKeys();

        // first access to index, do bsearch
        currentIndex = search(key);
        if (currentIndex < 0 && Math.abs(currentIndex) >= duplicatesOffset) {
            return -1;
        }
        currentIndex = Math.abs(currentIndex);
        return currentRow;
    }

    /**
     * Returns the row for keys that are equal or lower then the search key or -1 if there is no match.
     * <p/>
     * Consecutive calls to the method return the next matching keys.
     *
     * @param key the search key.
     * @return the matching row or -1.
     * @throws IOException
     * @throws DataFormatException
     */
    public int getOrLower(Key key) throws IOException, DataFormatException {
        // matched before and duplicates
        if (moreDuplicates()) return currentRow;

        // matched before get next smaller element (or no more duplicates)
        if (currentIndex > 0) return moreSmallerKeys();


        // first access to index, do bsearch
        currentIndex = search(key);
        // no exact match
        if (currentIndex < 0) {
            // all larger than key
            if (currentIndex == -1) {
                return -1;
            }

            // between or all smaller
            currentIndex = Math.abs(currentIndex);
            syncAndGet(--currentIndex);
        }

        return currentRow;
    }

    /**
     * Returns the row for keys that are lower then the search key or -1 if there is no match.
     * <p/>
     * Consecutive calls to the method return the next matching keys.
     *
     * @param key the search key.
     * @return the matching row or -1.
     * @throws IOException
     * @throws DataFormatException
     */
    public int getLower(Key key) throws IOException, DataFormatException {
        // matched before and duplicates
        if (moreDuplicates()) return currentRow;

        // matched before get next smaller element (or no more duplicates)
        if (currentIndex > 0) {
            return moreSmallerKeys();
        }


        // first access to index, do bsearch
        currentIndex = search(key);
        if (currentIndex < 0) {
            // all larger
            if (currentIndex == -1) {
                return -1;
            }
            // between or all smaller
            currentIndex = Math.abs(currentIndex);
            syncAndGet(--currentIndex);
        } else {
            // matched, get next smaller
            currentIndex--;
            if (currentIndex < 1) {
                return -1;
            }
            syncAndGet(currentIndex);
        }
        // get value
        return currentRow;
    }

    /**
     * Returns the row for keys that are equal to the search key or -1 if there is no match.
     * <p/>
     * Consecutive calls to the method return the next matching keys.
     *
     * @param key the search key.
     * @return the matching row or -1.
     * @throws IOException
     * @throws DataFormatException
     */
    public int get(Key key) throws IOException, DataFormatException {
        // matched before
        if (moreDuplicates()) return currentRow;
        if (currentIndex > 0) return -1;

        // first access to index, do bsearch
        currentIndex = search(key);
        // no match
        if (currentIndex < 0) {
            return -1;
        }
        // first call match
        return currentRow;
    }

    private int moreLargerKeys() throws IOException, DataFormatException {
        // check next elem in array
        currentIndex++;
        if (currentIndex < duplicatesOffset) {
            syncAndGet(currentIndex);

            return currentRow;
        } else {
            return -1;
        }
    }

    private int moreSmallerKeys() throws IOException, DataFormatException {
        // check next elem in array
        currentIndex--;
        if (currentIndex > 0) {
            syncAndGet(currentIndex);

            return currentRow;
        } else {
            return -1;
        }
    }

    private boolean moreDuplicates() throws IOException, DataFormatException {
        if (currentDuplicateIndex > 0) {
            syncAndGet();
            if (currentRow > 0) {
                return true;
            }
        }
        return false;
    }

    // get the values for the current duplicate index
    private void syncAndGet() throws IOException, DataFormatException {
        if (numberOfDuplicatesLeft > 0) {
            currentRow = readInt(currentDuplicateIndex++);
            numberOfDuplicatesLeft--;
        } else {
            currentDuplicateIndex = -1;
            currentRow = -1;
        }
    }

    // get the value for the index
    private void syncAndGet(int index) throws IOException, DataFormatException {
        currentRow = readInt(index);

        if (currentRow < 0) {
            currentDuplicateIndex = duplicatesOffset +
                    ((currentRow == -1) ? 0 : (currentRow * -1));

            currentRow = readInt(currentDuplicateIndex++);
            numberOfDuplicatesLeft = currentRow - 1;
            currentRow = readInt(currentDuplicateIndex++);
        } else {
            currentDuplicateIndex = -1;
        }


        keys.sync(currentRow);
        currentValue = (Key) keys.nextValue();
    }

    /**
     * Binary search on the keys column.
     *
     * To obtain the elements in order the sorted rows array is used.
     *
     * @param key the search key
     * @return on match: the index of the row in the sorted rows array
     *         on miss : - all keys are smaller: -(duplicatesOffset + 1)
     *                   - all keys are larger : -1
     *                   - key is in between   : -(index of next larger element)
     * @throws IOException
     * @throws DataFormatException
     */
    private int search(Key key) throws IOException, DataFormatException {
        int low = 1;
        int high = duplicatesOffset - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            syncAndGet(mid);

            if (currentValue.compareTo(key) < 0)
                low = mid + 1;
            else if (currentValue.compareTo(key) > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        // first key greater then searched key or array length
        return -(low);  // key not found.
    }


    private int readInt(int index) {
        int offset = index * 4;

        return ((sortedRows[offset] & 0xff) << 24) +
                ((sortedRows[offset + 1] & 0xff) << 16) +
                ((sortedRows[offset + 2] & 0xff) << 8) +
                ((sortedRows[offset + 3] & 0xff));
    }
}