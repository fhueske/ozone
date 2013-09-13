package eu.stratosphere.pact.common.io.pax.selection;

import java.io.IOException;
import java.util.Arrays;
import java.util.zip.DataFormatException;

import eu.stratosphere.pact.common.io.pax.IndexedPAXInputFormat;
import eu.stratosphere.pact.common.type.Key;

/**
 * A CSS Tree (Cache Search Tree) which holds a column as values.
 *
 * @author Andreas Kunft
 */
public class ColumnCSSTree {

    // KEY DATA

    // DO NOT CHANGE THIS VALUE, AS THE IMPLEMENTATION OF
    // THE SEARCH METHOD IS HARD CODED, EXPECTING 16 NODES.
    private static final int KEYS_PER_NODE = 16;

    private final int[] sortedArray;

    private final int sortedArraySize;

    private final int[] internalNodes;

    private final int firstLeafNode;

    private final int firstLeafNodeInBottomLvl;

    private final boolean sortedAscending;

    // COLUMN DATA

    private final IndexedPAXInputFormat.Column values;

    private Key currentValue;

    private int currentIndex = -1;

    private int sameValueIndex = -1;

    /**
     * Creates a CSS tree structure on top of the sorted array.
     *
     * @param sortedArray the array with the sorted values
     */
    public ColumnCSSTree(int[] sortedArray, boolean ascending, IndexedPAXInputFormat.Column column) {

        this.sortedAscending = ascending;
        this.values = column;
        this.sortedArraySize = sortedArray.length;
        int numberOfLeafNodes = (int) Math.ceil(sortedArraySize / (double) KEYS_PER_NODE);

        // add padding to the array to avoid IndexOutOfBoundExceptions in the search
        if (numberOfLeafNodes * KEYS_PER_NODE > sortedArraySize) {
            this.sortedArray = Arrays.copyOf(sortedArray, numberOfLeafNodes * KEYS_PER_NODE);
            for (int i = sortedArraySize; i < this.sortedArray.length; i++) {
                this.sortedArray[i] = this.sortedArray[sortedArraySize - 1];
            }
        } else {
            this.sortedArray = sortedArray;
        }

        // helper
        int branch = KEYS_PER_NODE + 1;
        int mask = KEYS_PER_NODE - 1;

        int k = (int) Math.ceil(Math.log10(numberOfLeafNodes) / Math.log10(branch));

        int mk = (int) Math.pow((branch), k);

        // The first leaf node in the bottom lvl of the tree.
        // The offset includes the internal nodes in front of the leaf nodes.
        int firstLeafInBottomLevel = (mk - 1) / KEYS_PER_NODE;

        // The offset / length of the leaf nodes of the lvl over the bottom lvl.
        int offset = (int) Math.floor((mk - numberOfLeafNodes) / (double) KEYS_PER_NODE);

        // The total number of internal nodes (= offset for the start of leaf nodes).
        int numberOfInternalNodes = firstLeafInBottomLevel - offset;

        int[] b = new int[numberOfInternalNodes * KEYS_PER_NODE];

        for (int i = numberOfInternalNodes - 1; i >= 0; i--) {
            for (int j = i * KEYS_PER_NODE + mask; j >= i * KEYS_PER_NODE; j--) {
                int child = branch * i + (j & mask) + 1;
                while (child < numberOfInternalNodes)
                    child = branch * child + branch;
                int l = child - firstLeafInBottomLevel;     // l is the node index
                l = l * KEYS_PER_NODE;     // l is the entry index
                if (l < 0) {
                    l += sortedArraySize;
                    b[j] = this.sortedArray[l + mask];
                } else {
                    if (l < sortedArraySize - offset * KEYS_PER_NODE) {
                        try {
                            b[j] = this.sortedArray[l + mask];
                        } catch (Exception e) {
                            b[j] = this.sortedArray[sortedArraySize - offset * KEYS_PER_NODE - 1];
                        }
                    } else
                        b[j] = this.sortedArray[sortedArraySize - offset * KEYS_PER_NODE - 1];
                }
            }
        }

        // from last to first internal node
        this.firstLeafNode = numberOfInternalNodes;
        this.firstLeafNodeInBottomLvl = firstLeafInBottomLevel;
        this.internalNodes = b;
    }

    /**
     * The last value retrieved from the column.
     *
     * @return the last value retrieved from the column.
     */
    public Key getCurrentValue() {
        return currentValue;
    }


    public int get(Key key) throws IOException, DataFormatException {
        // had match before
        // check for duplicates
        if (currentIndex != -1) {
            currentIndex++;
            if (currentIndex < sortedArraySize) {
                syncAndGet();
                if (key.equals(currentValue)) {
                    return sortedArray[currentIndex];
                }
            }

            currentIndex = -1;
            return -1;
        }

        try {
            currentIndex = search(key);
            syncAndGet();

            if (key.equals(currentValue)) {
                return sortedArray[currentIndex];
            }

            currentIndex = -1;
            return -1;

        } catch (ArrayIndexOutOfBoundsException e) {
            // happens if the key is larger than largest entry
            currentIndex = -1;
            return -1;
        }
    }

    public int getOrLower(Key key) throws IOException, DataFormatException {
        if (currentIndex != -1) {
            // check for duplicates
            if (sameValueIndex != -1) {
                currentIndex++;
                if (currentIndex < sortedArraySize) {
                    syncAndGet();
                    if (currentValue.equals(key)) {
                        return sortedArray[currentIndex];
                    } else {
                        // go back to first entry of same value and check smaller values
                        currentIndex = sameValueIndex;
                        sameValueIndex = -1;
                    }
                }
            }

            currentIndex--;
            if (currentIndex >= 0) {
                syncAndGet();
                return sortedArray[currentIndex];
            }

            currentIndex = -1;
            return -1;
        }

        try {
            currentIndex = search(key);
            syncAndGet();

            if (key.equals(currentValue)) {
                sameValueIndex = currentIndex;
                return sortedArray[currentIndex];
            }
            if (currentIndex == 0) {
                currentIndex = -1;
                return -1;
            }

            --currentIndex;
            syncAndGet();
            return sortedArray[currentIndex];
        } catch (ArrayIndexOutOfBoundsException e) {
            currentIndex = sortedArraySize - 1;
            // happens if the key is larger than largest entry
            return sortedArray[currentIndex];
        }
    }

    public int getLower(Key key) throws IOException, DataFormatException {
        if (currentIndex != -1) {
            // check for duplicates

            currentIndex--;
            if (currentIndex >= 0) {
                syncAndGet();
                return sortedArray[currentIndex];
            }

            currentIndex = -1;
            return -1;
        }

        try {
            currentIndex = search(key);
            syncAndGet();

            while (key.compareTo(currentValue) <= 0) {
                currentIndex--;
                if (currentIndex >= 0) {
                    syncAndGet();
                } else {
                    currentIndex = -1;
                    return -1;
                }
            }

            return sortedArray[currentIndex];

        } catch (ArrayIndexOutOfBoundsException e) {
            currentIndex = sortedArraySize - 1;
            // happens if the key is larger than largest entry
            return sortedArray[currentIndex];
        }
    }

    public int getOrHigher(Key key) throws IOException, DataFormatException {
        if (currentIndex != -1) {
            currentIndex++;
            if (currentIndex < sortedArraySize) {
                syncAndGet();
                return sortedArray[currentIndex];
            }

            currentIndex = -1;
            return -1;
        }

        try {
            currentIndex = search(key);
            syncAndGet();

            return sortedArray[currentIndex];

        } catch (ArrayIndexOutOfBoundsException e) {
            currentIndex = sortedArraySize - 1;
            // happens if the key is larger than largest entry
            return sortedArray[currentIndex];
        }
    }

    public int getHigher(Key key) throws IOException, DataFormatException {
        if (currentIndex != -1) {
            currentIndex++;
            if (currentIndex < sortedArraySize) {
                syncAndGet();
                return sortedArray[currentIndex];
            }

            currentIndex = -1;
            return -1;
        }

        try {
            currentIndex = search(key);
            syncAndGet();

            // get next higher value
            while (key.compareTo(currentValue) >= 0) {
                currentIndex++;
                if (currentIndex < sortedArraySize) {
                    syncAndGet();
                } else {
                    currentIndex = -1;
                    return -1;
                }
            }

            return sortedArray[currentIndex];

        } catch (ArrayIndexOutOfBoundsException e) {
            currentIndex = sortedArraySize - 1;
            // happens if the key is larger than largest entry
            return sortedArray[currentIndex];
        }
    }

    private void syncAndGet() throws IOException, DataFormatException {
        values.sync(sortedArray[currentIndex]);
        currentValue = (Key) values.nextValue();
    }

    private void syncAndGet(int offset) throws IOException, DataFormatException {
        values.sync(offset);
        currentValue = (Key) values.nextValue();
    }

    //17-way search (each node has 16 keys)
    private int search(Key key) throws IOException, DataFormatException {
        int node = 0;
        int l = 0;
        while (node < firstLeafNode) {
            int p = node << 4;
            syncAndGet(internalNodes[p + 8]);
            if (currentValue.compareTo(key) >= 0) {
                syncAndGet(internalNodes[p + 3]);
                if (currentValue.compareTo(key) >= 0) {
                    syncAndGet(internalNodes[p + 1]);
                    if (currentValue.compareTo(key) >= 0) {
                        syncAndGet(internalNodes[p]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 1;
                        else
                            l = 2;
                    } else {  //internalNodes[p+1]<key
                        syncAndGet(internalNodes[p + 2]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 3;
                        else
                            l = 4;
                    }
                } else { //if (internalNodes[p+3]<key)
                    syncAndGet(internalNodes[p + 5]);
                    if (currentValue.compareTo(key) >= 0) {
                        syncAndGet(internalNodes[p + 4]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 5;
                        else
                            l = 6;
                    } else {
                        syncAndGet(internalNodes[p + 6]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 7;
                        else {
                            syncAndGet(internalNodes[p + 7]);
                            if (currentValue.compareTo(key) >= 0)
                                l = 8;
                            else
                                l = 9;
                        }
                    }
                }
            } else { //(internalNodes[p+8]<key) {
                syncAndGet(internalNodes[p + 12]);
                if (currentValue.compareTo(key) >= 0) {
                    syncAndGet(internalNodes[p + 10]);
                    if (currentValue.compareTo(key) >= 0) {
                        syncAndGet(internalNodes[p + 9]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 10;
                        else
                            l = 11;
                    } else {  //internalNodes[p+1]<key
                        syncAndGet(internalNodes[p + 11]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 12;
                        else
                            l = 13;
                    }
                } else { //if (internalNodes[p+3]<key)
                    syncAndGet(internalNodes[p + 14]);
                    if (currentValue.compareTo(key) >= 0) {
                        syncAndGet(internalNodes[p + 13]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 14;
                        else
                            l = 15;
                    } else {
                        syncAndGet(internalNodes[p + 15]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 16;
                        else
                            l = 17;
                    }
                }
            }
            node = node * 17 + l;
        }
        l = node - firstLeafNodeInBottomLvl;
        l = (l << 4);     // l is the entry index
        if (l < 0)
            l += sortedArraySize;

        return l + findInLeaf(l, key);
    }

    private int findInLeaf(int p, Key key) throws IOException, DataFormatException {
        int l;
        syncAndGet(sortedArray[p + 8]);
        if (currentValue.compareTo(key) >= 0) {
            syncAndGet(sortedArray[p + 3]);
            if (currentValue.compareTo(key) >= 0) {
                syncAndGet(sortedArray[p + 1]);
                if (currentValue.compareTo(key) >= 0) {
                    syncAndGet(sortedArray[p]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 0;
                    else
                        l = 1;
                } else {  //internalNodes[p+1]<key
                    syncAndGet(sortedArray[p + 2]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 2;
                    else
                        l = 3;
                }
            } else { //if (internalNodes[p+3]<key)
                syncAndGet(sortedArray[p + 5]);
                if (currentValue.compareTo(key) >= 0) {
                    syncAndGet(sortedArray[p + 4]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 4;
                    else
                        l = 5;
                } else {
                    syncAndGet(sortedArray[p + 6]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 6;
                    else {
                        syncAndGet(sortedArray[p + 7]);
                        if (currentValue.compareTo(key) >= 0)
                            l = 7;
                        else
                            l = 8;
                    }
                }
            }
        } else { //(internalNodes[p+8]<key) {
            syncAndGet(sortedArray[p + 12]);
            if (currentValue.compareTo(key) >= 0) {
                syncAndGet(sortedArray[p + 10]);
                if (currentValue.compareTo(key) >= 0) {
                    syncAndGet(sortedArray[p + 9]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 9;
                    else
                        l = 10;
                } else {  //internalNodes[p+1]<key
                    syncAndGet(sortedArray[p + 11]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 11;
                    else
                        l = 12;
                }
            } else { //if (internalNodes[p+3]<key)
                syncAndGet(sortedArray[p + 14]);
                if (currentValue.compareTo(key) >= 0) {
                    syncAndGet(sortedArray[p + 13]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 13;
                    else
                        l = 14;
                } else {
                    syncAndGet(sortedArray[p + 15]);
                    if (currentValue.compareTo(key) >= 0)
                        l = 15;
                    else
                        l = 16;
                }
            }
        }
        return l;
    }
}
