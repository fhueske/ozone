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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.pax.io.ByteDataInputStream;
import eu.stratosphere.pact.common.io.pax.io.compression.CompressionCodecs;
import eu.stratosphere.pact.common.io.pax.io.compression.IDecompressor;
import eu.stratosphere.pact.common.io.pax.selection.Composition;
import eu.stratosphere.pact.common.io.pax.selection.ISelection;
import eu.stratosphere.pact.common.io.pax.selection.LocalOperator;
import eu.stratosphere.pact.common.io.pax.selection.LogicalOperator;
import eu.stratosphere.pact.common.io.pax.selection.Predicate;
import eu.stratosphere.pact.common.io.pax.selection.SelectionBuilder;
import eu.stratosphere.pact.common.io.pax.selection.SelectionVisitor;
import eu.stratosphere.pact.common.io.pax.selection.Status;
import eu.stratosphere.pact.common.io.pax.selection.bloomfilter.BloomFilter;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;


/**
 * Input format reading the indexed pax format.
 * <p/>
 * A indexed pax format (ipf) consists of several row groups.
 * Row groups are blocks of specified size (default: 4 MB) with an header and the actual record data.
 * The records within a row group are saved in columnar format. Each column of an
 * row group is compressed separately, to support lazy compression only if the column data
 * really needed. Row groups are always completely contained in one file split, which in the
 * case of ipf always the default HDFS block size of 64 mb.
 * <p/>
 * The header of a row group contains the information needed for the retrieval of the
 * column data as well as indexing features, namely:
 * - low / high keys
 * - Bloom filter (optional)
 * - sorted columns (optional)
 * <p/>
 * To use this features one has to define selections on ipf.
 * {@link IndexedPAXInputFormat#configureRCFormat(eu.stratosphere.pact.common.contract.FileDataSource)}.
 * <p/>
 * For the concrete structure, see the paxformat.eps figure in the resources folder.
 *
 */
public class IndexedPAXInputFormat extends FileInputFormat {

    /**
     * CONFIGURATION CONSTANTS
     */

    public static final String COLUMN_NUM_PARAMETER = "pact.input.rc.column.number";

    public static final String COLUMN_TYPE_PARAMETER_PREFIX = "pact.input.rc.column.type_";

    public static final String COLUMN_POSITION_PARAMETER_PREFIX = "pact.input.rc.column.position_";

    public static final String SELECTION_NUM_PARAMETER = "pact.input.rc.buildPredicate.number";

    public static final String SELECTION_OPERATOR_PARAMETER_PREFIX = "pact.input.rc.buildPredicate.operator_";

    public static final String SELECTION_POSITION_PARAMETER_PREFIX = "pact.input.rc.buildPredicate.position_";

    public static final String SELECTION_TYPE_PARAMETER_PREFIX = "pact.input.rc.buildPredicate.type_";

    public static final String SELECTION_VALUE_PARAMETER_PREFIX = "pact.input.rc.buildPredicate.value_";

    public static final String PREDICATE_SELECTION_GROUP_PARAMETER_PREFIX = "pact.input.rc.buildPredicate.group_";

    public static final String SELECTION_GROUP_LOGICAL_OPERATOR_PREFIX = "pact.input.rc.buildPredicate.group.combinator_";

    public static final String SELECTION_GROUP_PARAMETER = "pact.input.rc.buildPredicate.group.id";

    public static final String SELECTION_SINGLE_PREDICATE = "pact.input.rc.buildPredicate.single.predicate";


    /**
     * Number of columns in the records.
     */
    private int numberOfColumns;

    /**
     * The header of the current row group.
     */
    private Header header;

    /**
     * The column data of the current row group.
     */
    private Column[] columns;

    /**
     * The selection predicates, if there are any.
     */
    private ISelection selection;

    /**
     * The visitor to evaluate the selection, if there are any.
     */
    private SelectionVisitor visitor;

    /**
     * Flag to indicate the end of the file split.
     */
    private boolean endReached;

    /**
     * The current row which is processed.
     */
    private int currentRow;

    /**
     * The input stream for this file split.
     */
    private DataInputStream input;


    /**
     * Opens an input stream to the file defined in the input format.
     * The stream is positioned at the beginning of the given split.
     * <p/>
     * The stream is actually opened in an asynchronous thread to make sure any interruptions to the thread
     * working on the input format do not reach the file system.
     *
     * @see eu.stratosphere.pact.common.generic.io.InputFormat#open(eu.stratosphere.nephele.template.InputSplit)
     */
    @Override
    public void open(FileInputSplit split) throws IOException {
        // open split
        super.open(split);
        input = new DataInputStream(stream);

        // read header first
        readHeader();

        // sort the selection if defined
        if (selection != null) {
            sortSelection();
        }

        // update columns to represent all columns in the physical file
        // if not unused cols after the last defined column would not be skipped
        Column[] tmp = new Column[header.columnsInRowGroup];
        for (Column col : columns) {
            if (col != null) {
                tmp[col.columnPositionInRecord] = col;
            }
        }
        columns = tmp;
    }

    @Override
    public void close() throws IOException {
        super.close();
        header.paddingSize = 0;
        header.syncCheck = null;
        endReached = false;
    }

    /**
     * Reads the data of the row group header.
     *
     * @return true if the header was successfully read, false otherwise.
     */
    private boolean readHeader() {
        try {
            currentRow = -1;

            syncCheck();
            header.paddingSize = input.readInt();
            // check for skip
            // if an row group could not be added to the current
            if (header.paddingSize == IndexedPAXOutputFormat.SKIP) {
                return false;
            }
            // num sorted columns
            input.readInt();
            // num bloom filter columns
            input.readInt();
            header.records = input.readInt();
            header.columnsInRowGroup = input.readInt();
            header.uncompressedBytesPerColumn = new int[header.columnsInRowGroup];
            header.compressedBytesPerColumn = new int[header.columnsInRowGroup];
            //header.bytesPerField = new int[header.columnsInRowGroup][header.records];
            header.bytesPerFieldOffset = new int[header.columnsInRowGroup][header.records];

            for (int i = 0; i < header.columnsInRowGroup; i++) {
                fetchHeaderColumnData(i);
            }
        } catch (IOException e) {
            if (e instanceof EOFException) {
                return false;
            }
            throw new RuntimeException("Unable to read rc file header");
        }
        return true;
    }

    /**
     * Verify that the file split is part of an correct index pax format.
     *
     * @throws IOException
     */
    private void syncCheck() throws IOException {
        if (header.paddingSize > 0) {
            // indicates the end of an 64 mb block
            // as we always make the splits 64 mb long, this is the end of the split
            throw new EOFException();
        }
        // read in sync bytes in first check
        if (header.syncCheck == null) {
            readBlockHeader();

            // sync of first row group
            header.syncCheck = new byte[16];
            input.readFully(header.syncCheck);
        } else {
            // check sync
            byte[] readSync = new byte[16];
            input.readFully(readSync);
            if (!Arrays.equals(readSync, header.syncCheck)) {
                throw new RuntimeException("RC Input File corrupt: Sync check for row group failed");
            }
        }
    }

    /**
     * Verify the block header against the configuration values
     * and get the compression types for the columns.
     *
     * @throws IOException
     */
    private void readBlockHeader() throws IOException {
        // check for MAGIC
        byte[] magic = new byte[IndexedPAXOutputFormat.MAGIC_NUMBER.length];
        input.readFully(magic);
        if (!Arrays.equals(magic, IndexedPAXOutputFormat.MAGIC_NUMBER)) {
            throw new RuntimeException("RC Input Split has to start with: " + new String(IndexedPAXOutputFormat.MAGIC_NUMBER) + " (Wrong Offset)");
        }

        // get number of columns in pax file
        final int numberOfColumnsInPAX = input.readInt();

        // check for correct column classes
        for (int i = 0; i < numberOfColumnsInPAX; i++) {
            final byte type = input.readByte();
            if (i < columns.length && columns[i] != null) {
                Class<? extends Value> clazz = Utils.getValueClassForByte(type);
                if (columns[i].value.getClass() != clazz) {
                    throw new RuntimeException("Wrong type specified for column " + i + ": expected " + clazz + ", but was " + columns[i].value.getClass());
                }
            }
        }

        // get compression types for columns
        for (int i = 0; i < numberOfColumnsInPAX; i++) {
            final byte decompressorID = input.readByte();
            if (i < columns.length && columns[i] != null) {
                IDecompressor decompressor = CompressionCodecs.getDecompressorForID(decompressorID);
                columns[i].setDecompressor(decompressor);
            }
        }
    }

    /**
     * Reads the column specific data of the row group header.
     *
     * @param position the position of the column to read.
     * @throws IOException
     */
    private void fetchHeaderColumnData(int position) throws IOException {
        header.uncompressedBytesPerColumn[position] = input.readInt();
        header.compressedBytesPerColumn[position] = input.readInt();

        // if column is not used, skip further read
        int length = input.readInt();
        if (position >= columns.length || columns[position] == null) {
            input.skipBytes(length);
            return;
        }

        // feature flags (0x01 | 0x02 sorted, 0x04 bloom filter, 0x08 lh keys)
        byte flags = input.readByte();
        // sorting
        if ((flags & (OutputHeader.SORTED_ASC_FLAG | OutputHeader.SORTED_DSC_FLAG)) > 0) {
            SortedRow row = new SortedRow();
            row.rows = new byte[input.readInt() * 4];
            input.readFully(row.rows);
            row.order = (flags & OutputHeader.SORTED_ASC_FLAG) > 0;
            header.sortedColumns.put(position, row);
        }
        // bloom filter
        if ((flags & OutputHeader.BLOOM_FILTER_FLAG) > 0) {
            header.bloomFilterColumns.put(position, new BloomFilter(input));
        }

        // high low keys
        if ((flags & OutputHeader.LOW_HIGH_VALUES) > 0) {
            columns[position].high.read(input);
            columns[position].low.read(input);
        }

        int lastValueLength = input.readInt();
        //header.bytesPerField[position][0] = lastValueLength;
        header.bytesPerFieldOffset[position][0] = lastValueLength;
        for (int j = 1; j < header.records; ) {
            int val = input.readInt();
            if (val < 0) {
                // RLE
                for (int times = -1; times > val; times--) {
                    // length
                    //header.bytesPerField[position][j] = lastValueLength;
                    // offset
                    header.bytesPerFieldOffset[position][j] = header.bytesPerFieldOffset[position][j - 1] + lastValueLength;
                    j++;
                }
            } else {
                // length
                //header.bytesPerField[position][j] = val;
                // offset
                header.bytesPerFieldOffset[position][j] = header.bytesPerFieldOffset[position][j - 1] + val;

                lastValueLength = val;
                j++;
            }
        }
    }

    /**
     * Checks if the selection contains a predicate which works on a sorted column.
     * <p/>
     * If there is a column, and its not inside a disjunction it is moved to the head of
     * the root conjunction in order to exploit the sorting.
     */
    private void sortSelection() {
        Set<Integer> sortedColumns = header.sortedColumns.keySet();
        if (selection instanceof Predicate) {
            if (sortedColumns.contains(((Predicate) selection).position)) {
                ((Predicate) selection).setUseSorting(true);
            }
        } else {
            Composition root = (Composition) selection;
            // single disjunction -> no sorting
            if (root.getLogicalOperator() == LogicalOperator.OR) {
                return;
            }

            int sortedPredicatePosition = -1;
            for (int i = 0; i < root.getAtoms().size(); i++) {
                ISelection atom = root.getAtoms().get(i);
                // single conjunction | predicates & disjunctions
                if (atom instanceof Predicate) {
                    if (sortedColumns.contains(((Predicate) atom).position)) {
                        ((Predicate) atom).setUseSorting(true);
                        sortedPredicatePosition = i;
                        // return on first match
                        break;
                    }
                } else {
                    // return on first disjunction...
                    // as we arrange predicates in front there will only be disjunctions now
                    break;
                }
            }
            // move sorted predicate to front
            if (sortedPredicatePosition > 0) {
                ISelection sorted = root.getAtoms().remove(sortedPredicatePosition);
                root.getAtoms().add(0, sorted);
            }
        }
    }

    @Override
    public boolean nextRecord(PactRecord pactRecord) throws IOException {
        try {
            if (currentRow == -1) {
                readColumns();
                currentRow = 0;
                if (selection != null) {
                    visitor = new SelectionVisitor(pactRecord, header, columns, currentRow);
                }
            }
            if (selection != null) {
                return evaluateSelections(pactRecord);
            } else {
                for (Column col : columns) {
                    if (col != null) {
                        Value val = col.nextValue();
                        pactRecord.setField(col.columnPositionInOutput, val);
                    }
                }
                currentRow++;
                return true;
            }
        } catch (DataFormatException e) {
            throw new IOException("Could not decompress column data.", e);
        }
    }

    /**
     * Reads the actually column data into byte buffers.
     * <p/>
     * Note that the data is not decompressed yet, decompression happens on
     * the first access of the column values.
     *
     * @throws IOException
     */
    private void readColumns() throws IOException {
        int position = 0;
        for (Column col : columns) {
            // skip if the column is not projected
            if (col == null) {
                input.skipBytes(header.compressedBytesPerColumn[position]);
            } else {
                col.setBuffer(new byte[header.compressedBytesPerColumn[position]]);
                input.readFully(col.compressedBuffer);
            }
            position++;
        }
    }

    /**
     * Evaluates the column data against the selection predicates.
     *
     * @param pactRecord the pact record the values are written to on success.
     * @return true if a record was successfully evaluated, false otherwise.
     * @throws IOException
     */
    private boolean evaluateSelections(PactRecord pactRecord) throws IOException {
        try {
            boolean nextRowGroup = false;
            while (true) {
                // when sorting is used, the last row does not indicate a global mismatch
                if (nextRowGroup || (currentRow >= header.records && !visitor.useSorting())) {
                    if (readHeader()) {
                        readColumns();
                        currentRow = 0;
                        visitor = new SelectionVisitor(pactRecord, header, columns, currentRow);
                        nextRowGroup = false;
                    } else {
                        // EOF
                        endReached = true;
                        return false;
                    }
                }
                visitor.resetSetColumns();
                Status match = selection.accept(visitor);
                if (match != Status.NO_MATCH_GLOBAL) {
                    currentRow = visitor.incrementAndGetRow();
                    if (match == Status.MATCH) {
                        for (Column col : columns) {
                            if (col != null && !visitor.columnAlreadySet(col.columnPositionInOutput)) {
                                col.sync(visitor.getCurrentRow() - 1);
                                Value val = col.nextValue();
                                pactRecord.setField(col.columnPositionInOutput, val);
                            }
                        }
                        return true;
                    }
                } else {
                    nextRowGroup = true;
                }
            }
        } catch (Exception e) {
            throw new IOException("Failure during selection evaluation", e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (endReached) {
            return true;
        }
        // when sorting is used, the last row does not indicate a global mismatch
        if (currentRow >= header.records && (selection == null || !visitor.useSorting())) {
            if (!readHeader()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public FileBaseStatistics getStatistics(BaseStatistics baseStatistics) {
        return (eu.stratosphere.pact.generic.io.FileInputFormat.FileBaseStatistics) baseStatistics;
    }

    /**
     * In case of pax format we only allow splits of block size!
     *
     * @param minNumSplits The minimum desired number of file splits.
     * @return The computed file splits.
     * @see eu.stratosphere.pact.common.generic.io.InputFormat#createInputSplits(int)
     */
    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {

        final Path path = this.filePath;
        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

        final List<FileStatus> files = new ArrayList<FileStatus>();

		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] dir = fs.listStatus(path);
			for (int i = 0; i < dir.length; i++) {
				if (!dir[i].isDir()) {
					files.add(dir[i]);
				}
			}
		} else {
			files.add(pathFile);
		}
		
		for(FileStatus file : files) {

			long blockSize = fs.getDefaultBlockSize();
			long fileLength = file.getLen();
			
			if (fileLength > 0) {

	            // get the block locations and make sure they are in order with respect to their offset
	            final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, fileLength);
	            Arrays.sort(blocks);

	            int splitNum = 0;
	            int lastBlockIndex = 0;
	            
	            for (long offset = 0; offset < fileLength; offset += blockSize) {
	                // last split
	                long splitSize = (fileLength - offset < blockSize) ? fileLength - offset : blockSize;
	                HashSet<String> hosts = new HashSet<String>();
	                lastBlockIndex = getHostsForRCFile(blocks, offset, splitSize, lastBlockIndex, hosts);

	                FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), offset, splitSize, hosts.toArray(new String[hosts.size()]));
	                inputSplits.add(fis);
	            }
	        }
		}
		
        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }
    

    /**
     * In case of pax format we only allow splits of block size!
     *
     * @param minNumSplits The minimum desired number of file splits.
     * @return The computed file splits.
     * @see eu.stratosphere.pact.common.generic.io.InputFormat#createInputSplits(int)
     */
    /*
    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {

        final Path path = this.filePath;

        // get block size
        final FileStatus fileStatus = path.getFileSystem().getFileStatus(path);
        int blockSize = (int) fileStatus.getBlockSize();

        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

        final FileSystem fs = path.getFileSystem();
        final FileStatus file = fs.getFileStatus(path);

        if (file.isDir()) {
            // don't allow to specify dirs
            throw new RuntimeException("Only single RCFiles are allowed as Input");
        }

        final long totalLength = file.getLen();
        if (totalLength > 0) {

            // get the block locations and make sure they are in order with respect to their offset
            final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, totalLength);
            Arrays.sort(blocks);

            int splitNum = 0;
            int lastBlockIndex = 0;

            for (long offset = 0; offset < totalLength; offset += blockSize) {
                // last split
                long splitSize = (totalLength - offset < blockSize) ? totalLength - offset : blockSize;
                HashSet<String> hosts = new HashSet<String>();
                lastBlockIndex = getHostsForRCFile(blocks, offset, splitSize, lastBlockIndex, hosts);

                FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), offset, splitSize, hosts.toArray(new String[hosts.size()]));
                inputSplits.add(fis);
            }
        } else {
            // this should never happen!!!
            throw new RuntimeException("Empty file specified.");
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }
    */

    private int getHostsForRCFile(BlockLocation[] blocks, long offset, long splitSize, int startIndex, HashSet<String> hosts) throws IOException {

        int lastBlock = startIndex;
        // go over all indexes after the startIndex
        for (int i = startIndex; i < blocks.length; i++) {
            final long blockStart = blocks[i].getOffset();
            final long blockEnd = blockStart + blocks[i].getLength();

            if (blockEnd > offset || blockStart < offset + splitSize) {
                Collections.addAll(hosts, blocks[i].getHosts());
                lastBlock = i;
            }
        }
        if (hosts.isEmpty()) {
            throw new IllegalArgumentException("The given offset is not contained in the any block.");
        }

        return lastBlock;
    }

    /*
      * (non-Javadoc)
      * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
      */
    @Override
    public void configure(Configuration config) {

        super.configure(config);

        this.header = new Header();

        // read number of fields to parse
        numberOfColumns = config.getInteger(COLUMN_NUM_PARAMETER, -1);
        if (numberOfColumns < 1) {
            throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                    "Need to specify number of fields > 0.");
        }

        int[] columnPosIdx = new int[numberOfColumns];
        int maxColumnPos = -1;
        List<Integer> positions = new ArrayList<Integer>(numberOfColumns);

        // parse text positions
        for (int i = 0; i < numberOfColumns; i++) {
            int pos = config.getInteger(COLUMN_POSITION_PARAMETER_PREFIX + i, -1);
            if (pos == -1) {
                columnPosIdx[i] = i;
                maxColumnPos = i;
                positions.add(pos);
            } else {
                columnPosIdx[i] = pos;
                maxColumnPos = pos > maxColumnPos ? pos : maxColumnPos;
                positions.add(pos);
            }
        }

        // init columns
        this.columns = new Column[maxColumnPos + 1];
        for (int i = 0; i < numberOfColumns; i++) {

            Column col = new Column();
            col.columnPositionInRecord = columnPosIdx[i];
            col.columnPositionInOutput = i;

            @SuppressWarnings("unchecked")
            Class<? extends Value> clazz = (Class<? extends Value>) config.getClass(COLUMN_TYPE_PARAMETER_PREFIX + i, null);
            if (clazz == null) {
                throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                        "No value class for parameter " + i);
            }

            try {
                // low / high literals
                col.value = clazz.newInstance();
                if (col.value instanceof Key) {
                    col.low = (Key) clazz.newInstance();
                    col.high = (Key) clazz.newInstance();
                }


            } catch (InstantiationException ie) {
                throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                        "No value could not be instantiated for parameter " + i);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                        "No value could not be instantiated for parameter " + i);
            }

            columns[columnPosIdx[i]] = col;
        }

        // init selections
        int numPredicates = config.getInteger(SELECTION_NUM_PARAMETER, -1);
        if (numPredicates != -1) {

            int selectionGroups = config.getInteger(SELECTION_GROUP_PARAMETER, 0);
            List<List<ISelection>> predicatesPerGroup = new ArrayList<List<ISelection>>(selectionGroups + 1);
            for (int i = 0; i < selectionGroups + 1; i++) {
                predicatesPerGroup.add(new ArrayList<ISelection>());
            }
            for (int i = 0; i < numPredicates; i++) {

                // selection group
                int selectionGroup = config.getInteger(PREDICATE_SELECTION_GROUP_PARAMETER_PREFIX + i, -1);

                //operator
                String op = config.getString(SELECTION_OPERATOR_PARAMETER_PREFIX + i, null);
                if (op == null) {
                    throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                            "No operator class for parameter " + i);
                }
                // IllegalArgumentException not possible at this point for valueOf
                LocalOperator localOperator = LocalOperator.valueOf(op);

                // columnPositionInRecord
                int position = config.getInteger(SELECTION_POSITION_PARAMETER_PREFIX + i, -1);
                if (position < 0 || !positions.contains(position)) {
                    throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                            "Column positions have to > 0 and specified as field for parameter " + i);
                }

                // literals
                final Key value;
                @SuppressWarnings("unchecked")
                Class<? extends Key> cl = (Class<? extends Key>) config.getClass(SELECTION_TYPE_PARAMETER_PREFIX + i, null);
                if (cl == null) {
                    throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                            "No valid key class for parameter " + i);
                }

                // Check if the value is the correct type for the column
                if (columns[position].value.getClass() != cl) {
                    throw new RuntimeException("Invalid configuration for RCInputFormat: " +
                            "Incorrect type for selection in column: " + position);
                }

                try {
                    value = cl.newInstance();
                } catch (InstantiationException ie) {
                    throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                            "No value could not be instantiated for parameter " + i);
                } catch (IllegalAccessException e) {
                    throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                            "No value could not be instantiated for parameter " + i);
                }

                String selectionValue = config.getString(SELECTION_VALUE_PARAMETER_PREFIX + i, null);
                if (selectionValue == null) {
                    throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                            "No buildPredicate value for parameter " + i);
                }

                if (!Utils.setPactValueFromString(value, selectionValue)) {
                    throw new IllegalArgumentException("Invalid configuration for RCInputFormat: " +
                            "The buildPredicate value class is not supported for parameter " + i);
                }

                List<ISelection> predicates = predicatesPerGroup.get(selectionGroup);
                predicates.add(SelectionBuilder.buildSinglePredicate(localOperator, position, value));
            }


            // if selectionGroups == 0 there is only one predicate
            if (config.getBoolean(SELECTION_SINGLE_PREDICATE, false)) {
                selection = predicatesPerGroup.get(0).get(0);
            } else {
                SelectionBuilder builder = SelectionBuilder.create();
                // root
                String opString = config.getString(SELECTION_GROUP_LOGICAL_OPERATOR_PREFIX + 0, "");
                builder.startComposition(LogicalOperator.valueOf(opString));
                builder.predicates(predicatesPerGroup.get(0));
                for (int i = 1; i <= selectionGroups; i++) {
                    opString = config.getString(SELECTION_GROUP_LOGICAL_OPERATOR_PREFIX + i, "");
                    LogicalOperator op = LogicalOperator.valueOf(opString);
                    builder.startComposition(op).predicates(predicatesPerGroup.get(i)).endComposition();
                }
                builder.endComposition();
                selection = builder.build();
            }
        }
    }

    /**
     * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
     * fashion.
     *
     * @return A config builder for setting parameters.
     */
    public static ConfigBuilder configureRCFormat(FileDataSource target) {
        return new ConfigBuilder(target.getParameters());
    }

    /**
     * An abstract builder used to set parameters to the input format's configuration in a fluent way.
     */
    protected static class AbstractConfigBuilder<T> extends FileInputFormat.AbstractConfigBuilder<T> {

        /**
         * Creates a new builder for the given configuration.
         *
         * @param config The configuration into which the parameters will be written.
         */
        protected AbstractConfigBuilder(Configuration config) {
            super(config);
        }

        /**
         * Defines a column used in the InputFormat.
         *
         * @param columnType     the type of the column
         * @param columnPosition the columnPositionInRecord of the column, starting from 0
         * @return the ConfigBuilder itself
         */
        public T field(Class<? extends Value> columnType, int columnPosition) {
            final int numYet = config.getInteger(COLUMN_NUM_PARAMETER, 0);
            config.setClass(COLUMN_TYPE_PARAMETER_PREFIX + numYet, columnType);
            config.setInteger(COLUMN_POSITION_PARAMETER_PREFIX + numYet, columnPosition);
            config.setInteger(COLUMN_NUM_PARAMETER, numYet + 1);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Defines a Predicate for the input.
         *
         * @param selection a buildPredicate for the input
         * @return the ConfigBuilder itself
         */
        public T selection(ISelection selection) {
            if (selection instanceof Composition) {
                buildComposition(selection);
            } else {
                config.setInteger(SELECTION_GROUP_PARAMETER, 0);
                config.setBoolean(SELECTION_SINGLE_PREDICATE, true);
                Predicate predicate = (Predicate) selection;
                buildPredicate(0, predicate.operator, predicate.position, predicate.literal);
            }

            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        private void buildComposition(ISelection selection) {
            final int selectionGroup = config.getInteger(SELECTION_GROUP_PARAMETER, -1);
            if (!(selection instanceof Predicate)) {
                Composition composition = (Composition) selection;
                // node
                config.setInteger(SELECTION_GROUP_PARAMETER, selectionGroup + 1);
                config.setString(SELECTION_GROUP_LOGICAL_OPERATOR_PREFIX + (selectionGroup + 1), composition.getLogicalOperator().name());

                for (ISelection child : composition.getAtoms()) {
                    buildComposition(child);
                }
            } else {
                Predicate s = (Predicate) selection;
                buildPredicate(selectionGroup, s.operator, s.position, s.literal);
            }
        }

        private void buildPredicate(int selectionGroup, LocalOperator operator, int selectionPosition, Key selectionValues) {
            final int numYet = config.getInteger(SELECTION_NUM_PARAMETER, 0);
            config.setInteger(PREDICATE_SELECTION_GROUP_PARAMETER_PREFIX + numYet, selectionGroup);
            config.setString(SELECTION_OPERATOR_PARAMETER_PREFIX + numYet, operator.name());
            config.setInteger(SELECTION_POSITION_PARAMETER_PREFIX + numYet, selectionPosition);
            config.setClass(SELECTION_TYPE_PARAMETER_PREFIX + numYet, selectionValues.getClass());
            config.setString(SELECTION_VALUE_PARAMETER_PREFIX + numYet, selectionValues.toString());
            config.setInteger(SELECTION_NUM_PARAMETER, numYet + 1);
        }
    }

    /**
     * A builder used to set parameters to the input format's configuration in a fluent way.
     */
    public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder> {
        /**
         * Creates a new builder for the given configuration.
         *
         * @param targetConfig The configuration into which the parameters will be written.
         */
        protected ConfigBuilder(Configuration targetConfig) {
            super(targetConfig);
        }

    }

    //-----------------------------------------------------------------------------------------------
    // INNER CLASSES
    //-----------------------------------------------------------------------------------------------

    private class SortedRow {
        public boolean order;
        public byte[] rows;
    }

    /**
     * Header data of the row group.
     */
    public class Header {

        private byte[] syncCheck;
        private int records;
        private int columnsInRowGroup;
        private int paddingSize;
        private int[] compressedBytesPerColumn;
        private int[] uncompressedBytesPerColumn;
        //private int[][] bytesPerField;
        private int[][] bytesPerFieldOffset;

        public Map<Integer, SortedRow> sortedColumns = new HashMap<Integer, SortedRow>();

        public boolean isSorted(int column) {
            return sortedColumns.containsKey(column);
        }

        public byte[] getSortedRows(int column) {
            return sortedColumns.get(column).rows;
        }

        public boolean getOrder(int column) {
            return sortedColumns.get(column).order;
        }

        public Map<Integer, BloomFilter> bloomFilterColumns = new HashMap<Integer, BloomFilter>();

        public boolean hasBloomFilter(int column) {
            return bloomFilterColumns.containsKey(column);
        }
    }

    /**
     * A Column in the row group.
     * <p/>
     * The byte buffer containing the column values can be resetSetColumns
     * to avoid object creation.
     * Compressed buffers are decompressed on the first access.
     */
    public class Column implements Comparable<Column> {

        private ByteDataInputStream in;

        private IDecompressor decompressor;

        public int columnPositionInRecord;
        
        public int columnPositionInOutput;

        public Value value;

        public Key high;

        public Key low;

        private byte[] buffer;

        private byte[] compressedBuffer;

        private boolean newCompressedBuffer;

        /**
         * The index of the next value returned by the column.
         */
        int currentFieldIndex;

        private int offset;

        Column() {
            this.newCompressedBuffer = false;
        }

        /**
         * Sets the decompressor instance used to decompress the column.
         *
         * @param decompressor the decompressor used to decompress the column.
         */
        public void setDecompressor(IDecompressor decompressor) {
            this.decompressor = decompressor;
        }

        /**
         * Sets the compressed byte buffer containing the column values of the current row group.
         *
         * @param buffer the byte buffer with the compressed column values.
         */
        public void setBuffer(byte[] buffer) {
            this.compressedBuffer = buffer;
            newCompressedBuffer = true;
            currentFieldIndex = 0;
            offset = 0;
        }

        /**
         * Sets the buffer to save the uncompressed values of the column.
         *
         * @param buffer the byte buffer for the uncompressed values of the column.
         */
        private void setUncompressedBuffer(byte[] buffer) throws IOException {
            this.buffer = buffer;
            if (in == null) {
                in = new ByteDataInputStream(buffer);
            } else {
                in.changeBuffer(buffer);
            }
        }

        /**
         * Decompress the values.
         */
        private void decompress() throws IOException {
            setUncompressedBuffer(new byte[header.uncompressedBytesPerColumn[columnPositionInRecord]]);
            decompressor.decompress(compressedBuffer, this.buffer);
            newCompressedBuffer = false;
        }

        /**
         * The next value in the column.
         * If the column is not yet decompressed, decompression is done first.
         *
         * @return the next value in the column.
         */
        public Value nextValue() throws IOException, DataFormatException {
            // decompression and first time access
            if (newCompressedBuffer) {
                decompress();
            }
            value.read(in);
            currentFieldIndex++;
            return value;
        }

        /**
         * Moves the index in the column to to the specified index.
         *
         * @param index
         */
        public void sync(int index) throws IOException, DataFormatException {
            int sign = Integer.signum(index - currentFieldIndex);
            if (sign == 0) {
                return;
            }
            if (newCompressedBuffer) {
                decompress();
            }
            if (sign > 0) {
                final int skip;
                if (currentFieldIndex == 0) {
                    skip = header.bytesPerFieldOffset[columnPositionInRecord][index - 1];
                } else {
                    skip = header.bytesPerFieldOffset[columnPositionInRecord][index - 1] - header.bytesPerFieldOffset[columnPositionInRecord][currentFieldIndex - 1];
                }
                in.skip(skip);
            } else {
                final int rewind;
                if (index == 0) {
                    rewind = header.bytesPerFieldOffset[columnPositionInRecord][currentFieldIndex - 1];
                } else {
                    rewind = header.bytesPerFieldOffset[columnPositionInRecord][currentFieldIndex - 1] - header.bytesPerFieldOffset[columnPositionInRecord][index - 1];
                }
                in.rewind(rewind);
            }
            currentFieldIndex = index;
        }

        @Override
        public int compareTo(Column o) {
            if (this.columnPositionInRecord < o.columnPositionInRecord)
                return -1;
            if (this.columnPositionInRecord > o.columnPositionInRecord)
                return 1;
            return 0;
        }
    }
}
