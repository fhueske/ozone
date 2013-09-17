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

package eu.stratosphere.pact.common.io.pax;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeSet;

import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.type.Key;

/**
 * Sorts <row, Key> pairs compared by their values.
 *
 */
public class Sorter {

    private static class Element {

        public final int row;

        public final Key key;

        Element(int row, Key key) {
            this.row = row;
            this.key = key;
        }
    }

    private final Order order;

    private final TreeSet<Element> tree;

    private int[] sortedRows;

    public Sorter(Order order) {
        final Comparator<Element> comparator = (order == Order.ASCENDING) ? new Comparator<Element>() {
            @Override
            public int compare(Element o1, Element o2) {
                int comp = o1.key.compareTo(o2.key);
                return comp != 0 ? comp : o1.row - o2.row;
            }
        } : new Comparator<Element>() {
            @Override
            public int compare(Element o1, Element o2) {
                int comp = o2.key.compareTo(o1.key);
                return comp != 0 ? comp : o2.row - o1.row;
            }
        };
        this.order = order;
        this.tree = new TreeSet<Element>(comparator);
    }

    public Order getOrder() {
        return order;
    }


    public void add(int currentRow, Key key) {
        tree.add(new Element(currentRow, key));
    }

    /**
     * Returns the rows sorted by their corresponding values.
     * <p/>
     * The finish() method has to be called before in order to obtain correct values.
     *
     * @return
     */
    public int[] getSortedRows() {
        return sortedRows;
    }

    /**
     * Creates an int array with the sorted rows.
     * <p/>
     * Rows with duplicate values are saved at the end of the sorted rows and an offset is saved instead.
     * The duplicate rows are saved with the number of duplicates prepended.
     */
    public void finish() {
        final LinkedList<Integer> uniqueVals = new LinkedList<Integer>();
        final LinkedList<Integer> duplicates = new LinkedList<Integer>();
        final LinkedList<Integer> allDuplicates = new LinkedList<Integer>();
        Key lastVal = null;
        for (Element elem : tree) {
            if (lastVal != null && elem.key.equals(lastVal)) {
                duplicates.add(elem.row);
            } else {
                if (!duplicates.isEmpty()) {
                    int row = uniqueVals.removeLast();
                    duplicates.addFirst(row);
                    uniqueVals.add((allDuplicates.isEmpty()) ? -1 : allDuplicates.size() * -1);
                    allDuplicates.add(duplicates.size());
                    allDuplicates.addAll(duplicates);
                    duplicates.clear();
                }
                uniqueVals.add(elem.row);
                lastVal = elem.key;
            }
        }
        if (!duplicates.isEmpty()) {
            int row = uniqueVals.removeLast();
            duplicates.addFirst(row);
            uniqueVals.add((allDuplicates.isEmpty()) ? -1 : allDuplicates.size() * -1);
            allDuplicates.add(duplicates.size());
            allDuplicates.addAll(duplicates);
            duplicates.clear();
        }

        int[] retval = new int[2 + uniqueVals.size() + allDuplicates.size()];
        retval[0] = uniqueVals.size() + allDuplicates.size() + 1;
        retval[1] = uniqueVals.size();
        int index = 2;
        for (int i : uniqueVals) {
            retval[index++] = i;
        }
        for (int i : allDuplicates) {
            retval[index++] = i;
        }

        sortedRows = retval;
    }

    public void reset() {
        tree.clear();
    }

    public int getSizeOnDisk() {
        return sortedRows.length * 4;
    }
}
