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

    private final byte[] rows;

    private final int uniqueRowsLength;

    // COLUMN DATA

    private final IndexedPAXInputFormat.Column keys;

    private Key currentValue;

    private int currentIndex = -1;

    private int currentDuplicateIndex = -1;

    private int currentRow = -1;

    private int numberOfDuplicatesLeft;

    private final int duplicatesOffset;

    /**
     * Creates the structure for the supplied rows and keys.
     *
     * @param rows the sorted rows for the keys.
     * @param keys the keys for the sorted rows
     */
    public BSearch(byte[] rows, IndexedPAXInputFormat.Column keys) {
        this.rows = rows;
        this.keys = keys;
        this.uniqueRowsLength = (((rows[0] & 0xff) << 24) + ((rows[1] & 0xff) << 16) + ((rows[2] & 0xff) << 8) + (rows[3] & 0xff));

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
        // matched before and duplicates
        if (currentDuplicateIndex > 0) {
            syncAndGet();
            if (currentRow > 0) {
                return currentRow;
            }
        }

        // second call
        if (currentIndex > 0) {
            // check next elem in array
            currentIndex++;
            if (currentIndex < duplicatesOffset) {
                syncAndGet(currentIndex);

                return currentRow;
            } else {
                return -1;
            }
        }


        // first access to index, do bsearch
        currentIndex = search(key);
        if (currentIndex < 0) {
            // no match, check if in bound
            if (Math.abs(currentIndex) >= duplicatesOffset) {
                return -1;
            }
            currentIndex = Math.abs(currentIndex);
            // match get next higher
        } else {
            currentIndex++;
            if (currentIndex >= duplicatesOffset) {
                return -1;
            }
        }
        // get value
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
        // matched before and duplicates
        if (currentDuplicateIndex > 0) {
            syncAndGet();
            if (currentRow > 0) {
                return currentRow;
            }
        }

        // second call
        if (currentIndex > 0) {
            // check next elem in array
            currentIndex++;
            if (currentIndex < duplicatesOffset) {
                syncAndGet(currentIndex);

                return currentRow;
            } else {
                return -1;
            }
        }


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
        if (currentDuplicateIndex > 0) {
            syncAndGet();
            if (currentRow > 0) {
                return currentRow;
            }
        }

        // second call
        if (currentIndex > 0) {
            // check next elem in array
            currentIndex--;
            if (currentIndex > 0) {
                syncAndGet(currentIndex);

                return currentRow;
            } else {
                return -1;
            }
        }


        // first access to index, do bsearch
        currentIndex = search(key);
        if (currentIndex < 0) {
            if (currentIndex == -1) {
                return -1;
            }
            currentIndex = Math.abs(currentIndex);
            if (currentValue.compareTo(key) > 0) {
                syncAndGet(--currentIndex);
            }
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
        if (currentDuplicateIndex > 0) {
            syncAndGet();
            if (currentRow > 0) {
                return currentRow;
            }
        }

        // second call
        if (currentIndex > 0) {
            // check next elem in array
            currentIndex--;
            if (currentIndex > 0) {
                syncAndGet(currentIndex);

                return currentRow;
            } else {
                return -1;
            }
        }


        // first access to index, do bsearch
        currentIndex = search(key);
        if (currentIndex < 0) {
            // no match, check if in bound
            if (currentIndex == -1) {
                return -1;
            }
            currentIndex = Math.abs(currentIndex);
            if (currentValue.compareTo(key) >= 0) {
                syncAndGet(--currentIndex);
            }
            // match get next higher
        } else {
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
        // matched before and duplicates
        if (currentDuplicateIndex > 0) {
            syncAndGet();
            if (currentRow > 0) {
                return currentRow;
            }

            currentIndex = -1;
            return -1;
        }

        // matched before has no duplicates
        if (currentIndex > 0) {
            return -1;
        }

        // first access to index, do bsearch
        currentIndex = search(key);
        if (currentIndex < 0) {
            return -1;
        }

        return currentRow;
    }

    private void syncAndGet() throws IOException, DataFormatException {
        if (numberOfDuplicatesLeft > 0) {
            currentRow = readInt(currentDuplicateIndex++);
            numberOfDuplicatesLeft--;
        } else {
            currentDuplicateIndex = -1;
            currentRow = -1;
        }
    }

    private void syncAndGet(int offset) throws IOException, DataFormatException {
        currentRow = readInt(offset);

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

        return ((rows[offset] & 0xff) << 24) +
                ((rows[offset + 1] & 0xff) << 16) +
                ((rows[offset + 2] & 0xff) << 8) +
                ((rows[offset + 3] & 0xff));
    }
}
