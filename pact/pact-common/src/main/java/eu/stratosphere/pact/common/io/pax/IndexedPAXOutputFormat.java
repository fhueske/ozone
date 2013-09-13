package eu.stratosphere.pact.common.io.pax;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.pax.io.compression.CompressionCodecs;
import eu.stratosphere.pact.common.io.pax.io.compression.ICompressor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Output format to write Indexed PAX Format.
 * <p/>
 * The format only handles the writing process of the different row groups.
 * The header data is handled by the OutputHeader class.
 * The column data is handled by the OutputColumn class.
 * <p/>
 * For the concrete binary structure, see the paxformat.eps figure in the resources folder.
 *
 * @author Andreas Kunft
 */
public class IndexedPAXOutputFormat extends FileOutputFormat {

    // CONFIGURATION PARAMETERS

    private static final String NUM_COLUMNS_PARAMETER = "pact.output.rc.num-cols";

    private static final String COLUMN_TYPE_PARAMETER_PREFIX = "pact.output.rc.type_";

    private static final String COLUMN_COMPRESSION_TYPE_PARAMETER_PREFIX = "pact.output.rc.compression_";

    private static final String RECORD_POSITION_PARAMETER_PREFIX = "pact.output.rc.position_";

    private static final String LENIENT_PARSING = "pact.output.rc.lenient";

    private static final String ROW_GROUP_SIZE_PARAMETER = "pact.output.rc.rowgroupsize";

    private static final String SORT_COLUMNS_PARAMETER = "pact.output.rc.sorted.column.num";

    private static final String SORT_COLUMN_POSITION_PREFIX = "pact.output.rc.sorted.column_";

    private static final String SORT_COLUMN_ASC_PREFIX = "pact.output.rc.sorted.asc_";

    private static final String BLOOM_FILTER_COLUMN_PARAMETER = "pact.output.rc.bloomfilter.num";

    private static final String BLOOM_FILTER_COLUMN_POSITION_PREFIX = "pact.output.rc.bloomfilter.column_";

    private static final String NUMBER_OF_THREADS_PARAMETER = "pact.output.rc.num.threads";

    private static final String DEFAULT_COMPRESSION_TYPE_PARAMETER = "pact.output.rc.default.compression";

    // CONSTANTS

    public static final int DEFAULT_ROW_GROUP_SIZE = 4 * 1024 * 1024; // 4 MB row group compressedSize

    public static final byte[] MAGIC_NUMBER = new byte[]{'R', 'C', '#', '1'};

    public static final int SKIP = Integer.MIN_VALUE;

    private static final Log LOG = LogFactory.getLog(IndexedPAXOutputFormat.class);

    // CONFIGURATION VALUES

    private int numColumns;

    private boolean lenient;

    private int rowGroupSize;

    private int blockSize;

    // file data

    protected OutputHeader header;

    protected OutputColumn[] columns;

    // row group & block compressedSize control

    private int currentUncompressedBytesInColumns;

    private int allRowGroupsInCurrentBlock;

    private boolean newBlock;

    // output

    private BufferedOutputStream writer;

    private ExecutorService threadPool;

    private BlockingQueue<OutputColumn> columnsToCompress;

    private int numThreads;

    private ResetCountDownLatch latch;

    private volatile boolean exceptionInConsumerThread = false;

    /**
     * A Consumer compresses columns.
     */
    private static class Consumer implements Runnable {

        private final BlockingQueue<OutputColumn> queue;

        private final OutputHeader header;

        private final ResetCountDownLatch latch;

        private final IndexedPAXOutputFormat caller;

        public Consumer(IndexedPAXOutputFormat caller, ResetCountDownLatch latch, BlockingQueue<OutputColumn> queue, OutputHeader header) {
            this.caller = caller;
            this.latch = latch;
            this.queue = queue;
            this.header = header;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    OutputColumn column = queue.take();
                    column.compress();
                    header.setCompressedBytesPerColumn(column.getColumnPosition(), column.compressedSize());
                    if (column.isKeyColumn()) {
                        try {
                            header.setLowHighKeys(column.getColumnPosition(), column.getLowestValue(), column.getHighestValue());
                        } catch (IOException e) {
                            caller.exceptionInConsumerThread = true;
                        }
                    }
                    latch.countDown();

                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }


    /**
     * Open the output format.
     * <p/>
     * Only block sizes in int range are supported!
     *
     * @param taskNumber
     * @throws java.io.IOException
     */
    @Override
    public void open(int taskNumber) throws IOException {
        super.open(taskNumber);

        // could not get file block compressedSize cause nothing is written yet
        // and so FileStatus would return 0 instead of the block compressedSize
        blockSize = (int) super.outputFilePath.getFileSystem().getDefaultBlockSize();
        LOG.debug("Block compressedSize: " + blockSize + " bytes");

        writer = new BufferedOutputStream(stream);

        // init thread pool and latch

        threadPool = Executors.newFixedThreadPool(numThreads);
        latch = new ResetCountDownLatch(numColumns);

        // start consumer depending on the number of threads specified

        columnsToCompress = new ArrayBlockingQueue<OutputColumn>(numColumns);
        for (int i = 0; i < numThreads; i++) {
            threadPool.execute(new Consumer(this, latch, columnsToCompress, this.header));
        }

        newBlock = true;
    }

    /**
     * Close the output format.
     * <p/>
     * Values not yet written to disk, will be flushed before closing.
     *
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {
        // write back remaining bytes in buffer.
        writeBackRemaining();
        threadPool.shutdownNow();

        if (writer != null) {
            writer.close();
        }
        super.close();
    }

    @Override
    public void writeRecord(PactRecord pactRecord) throws IOException {
        final int numRecFields = pactRecord.getNumFields();

        int uncompressedRecordSize = 0;
        for (int i = 0; i < numColumns; i++) {
            final OutputColumn column = columns[i];
            final int readPos = column.getColumnPosition();
            final int colSize;
            if (readPos < numRecFields) {

                final Value value = pactRecord.getField(readPos, column.getValueClass());

                if (value == null) {
                    if (!this.lenient) {
                        throw new RuntimeException("Cannot serialize record with <null> value at columnPositionInRecord: " + readPos);
                    } else {
                        colSize = 0;
                    }
                } else {
                    colSize = column.addValue(value);
                    header.updateColumn(i, value, colSize);
                }
            } else {
                if (!this.lenient) {
                    throw new RuntimeException("Cannot serialize record with out field at columnPositionInRecord: " + readPos);
                }
                colSize = 0;
            }

            uncompressedRecordSize += colSize;
        }
        currentUncompressedBytesInColumns += uncompressedRecordSize;
        header.increaseRecords();

        final int newRowGroupSize = currentUncompressedBytesInColumns + header.sizeOnDisk();
        // check if record is small enough for current row group
        // or current row group has to be flushed to disk before
        if (newRowGroupSize > rowGroupSize || allRowGroupsInCurrentBlock + newRowGroupSize > blockSize) {

            // flush current containers to disk
            writeToDisk(allRowGroupsInCurrentBlock + newRowGroupSize > blockSize);
            header.reset();
            currentUncompressedBytesInColumns = 0;
        }
    }

    /**
     * Writes the current row group to disk.
     * <p/>
     * If the size of the uncompressed row group exceeds the
     * hdfs block size, the block is either added to the next
     * block or (most common case) its still does fit into the
     * current block due to compression.
     *
     * @param finishBlock indicates that the uncompressed row group would exceed the block size
     * @throws IOException
     */
    private void writeToDisk(final boolean finishBlock) throws IOException {

        for (OutputColumn column : columns) {
            columnsToCompress.offer(column);
        }
        // wait for all to compress
        try {
            latch.await();
            if (exceptionInConsumerThread) {
                throw new IOException("Failure while compressing columns.");
            }
            latch.reset(numColumns);
        } catch (InterruptedException e) {
            throw new IOException("Failure while compressing columns.");
        }

        // calculate real header size
        header.finish();

        int rowGroupSize = header.sizeOnDisk() + header.columnsSizeInBytes();

        int paddingSize;
        boolean skip = false;
        if (finishBlock) {
            paddingSize = blockSize - (allRowGroupsInCurrentBlock + rowGroupSize);

            // if the compression did not shorten the row group enough to fit in
            // the current file split, the row group is written to the next split
            if (paddingSize < 0) {

                writer.write(OutputHeader.SYNC_BLOCK);
                Utils.writeInt(writer, SKIP);
                // 4 = SKIP length
                int padding = paddingSize - OutputHeader.SYNC_BLOCK.length - 4 + rowGroupSize;
                writer.write(new byte[padding]);
                newBlock = true;
                paddingSize = 0;
                skip = true;

                LOG.debug("Skipping " + padding + " bytes in current split.");
            } else {
                LOG.debug("Padding " + paddingSize + " bytes in current split.");
            }

        } else {
            paddingSize = 0;
            allRowGroupsInCurrentBlock += rowGroupSize;
        }

        LOG.trace("Header compressedSize: " + header.sizeOnDisk() + ", Columns compressedSize: " + header.columnsSizeInBytes() + " Records: " + header.records());

        header.setPaddingSize(paddingSize);

        // RC SPLIT HEADER
        int blockHeaderSize = 0;
        if (newBlock) {
            blockHeaderSize = writeBlockHeader();
            allRowGroupsInCurrentBlock += blockHeaderSize;
        }

        // HEADER
        header.write(writer);

        // VALUES
        for (OutputColumn column : columns) {
            final byte[] tmp = column.getCompressedColumnValues();
            writer.write(tmp);
            column.resetBuffer();
        }

        // PADDING
        if (paddingSize > 0) {
            writer.write(new byte[paddingSize]);
        }

        if (finishBlock) {
            newBlock = !skip;

            // if the row group was written to a new split
            // its compressedSize is added
            allRowGroupsInCurrentBlock =
                    blockHeaderSize
                            + ((skip) ? header.sizeOnDisk() + header.columnsSizeInBytes() : 0);
        }

    }

    private int writeBlockHeader() throws IOException {
        writer.write(MAGIC_NUMBER);
        // write number of columns
        Utils.writeInt(writer, numColumns);
        // write column information
        for (OutputColumn col : columns) {
            writer.write(Utils.getByteForValueClass(col.getValueClass()));
        }
        // write compression information
        for (OutputColumn col : columns) {
            writer.write(CompressionCodecs.getCodecID(col.getCompressor()));
        }

        newBlock = false;

        return MAGIC_NUMBER.length +
                4 + // columns
                (columns.length * 2); // column types + compression types

    }

    /**
     * If there is a row group not yet written to disk,
     * it is forced to disk.
     *
     * @throws IOException
     */
    private void writeBackRemaining() throws IOException {
        if (currentUncompressedBytesInColumns != 0) {
            writeToDisk(false);
        }
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        // GENERAL SETTINGS
        // ------------------------------------------------------------------------

        this.numColumns = parameters.getInteger(NUM_COLUMNS_PARAMETER, -1);
        if (this.numColumns < 1) {
            throw new IllegalArgumentException("Invalid configuration for RCOutputFormat: " +
                    "Need to specify number of fields > 0.");
        }

        this.rowGroupSize = parameters.getInteger(ROW_GROUP_SIZE_PARAMETER, DEFAULT_ROW_GROUP_SIZE);
        this.lenient = parameters.getBoolean(LENIENT_PARSING, false);

        this.numThreads = Math.max(1, parameters.getInteger(NUMBER_OF_THREADS_PARAMETER, 1));

        @SuppressWarnings("unchecked")
        Class<? extends ICompressor> defaultCompressor = (Class<? extends ICompressor>) parameters.getClass(DEFAULT_COMPRESSION_TYPE_PARAMETER, CompressionCodecs.getGZip().getClass());

        // COLUMNS
        // ------------------------------------------------------------------------

        Set<OutputColumn> cols = new TreeSet<OutputColumn>();

        final int initialSize = rowGroupSize / numColumns;

        for (int i = 0; i < this.numColumns; i++) {

            // column type
            @SuppressWarnings("unchecked")
            Class<? extends Value> clazz = (Class<? extends Value>) parameters.getClass(COLUMN_TYPE_PARAMETER_PREFIX + i, null);
            if (clazz == null) {
                throw new IllegalArgumentException("Invalid configuration for RCOutputFormat: " +
                        "No type class for parameter " + i);
            }

            // column position
            int pos = parameters.getInteger(RECORD_POSITION_PARAMETER_PREFIX + i, Integer.MIN_VALUE);
            if (pos < 0) {
                throw new IllegalArgumentException("Invalid configuration for RCOutputFormat: " +
                        "No or negative position specified for parameter " + i);
            }

            // compression type
            @SuppressWarnings("unchecked")
            Class<? extends ICompressor> columnCompressorClass = (Class<? extends ICompressor>) parameters.getClass(
                    COLUMN_COMPRESSION_TYPE_PARAMETER_PREFIX + i, defaultCompressor
            );

            ICompressor compressor;
            try {
                compressor = columnCompressorClass.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid configuration for RCOutputFormat: " +
                        "Invalid compressor class specified for parameter " + i);
            }

            final OutputColumn column = new OutputColumn(clazz, pos, initialSize, compressor);
            cols.add(column);
        }
        columns = cols.toArray(new OutputColumn[cols.size()]);

        // HEADER
        // ------------------------------------------------------------------------

        // sorted columns
        final int numSortedColumns = parameters.getInteger(SORT_COLUMNS_PARAMETER, 0);
        final Map<Integer, Order> sortedColumns = new HashMap<Integer, Order>(numSortedColumns);
        for (int i = 0; i < numSortedColumns; i++) {
            final int position = parameters.getInteger(SORT_COLUMN_POSITION_PREFIX + i, -1);
            if (position >= numColumns) {
                throw new IllegalArgumentException("Invalid configuration for RecordOutputFormat: " +
                        "Specified sort column is out of range");
            }
            if (!Key.class.isAssignableFrom(columns[position].getValueClass())) {
                throw new IllegalArgumentException("Invalid configuration for RecordOutputFormat: " +
                        "Specified sort column is not of type Key");
            }
            final Order order = Order.valueOf(parameters.getString(SORT_COLUMN_ASC_PREFIX + i, Order.NONE.toString()));
            sortedColumns.put(position, order);
        }

        // bloom filter columns
        final int numBloomFilterColumns = parameters.getInteger(BLOOM_FILTER_COLUMN_PARAMETER, 0);
        final List<Integer> bloomFilterColumns = new ArrayList<Integer>(numBloomFilterColumns);
        for (int i = 0; i < numBloomFilterColumns; i++) {
            final int position = parameters.getInteger(BLOOM_FILTER_COLUMN_POSITION_PREFIX + i, -1);
            if (position >= numColumns) {
                throw new IllegalArgumentException("Invalid configuration for RecordOutputFormat: " +
                        "Specified bloom filter column is out of range");
            }
            bloomFilterColumns.add(position);
        }

        // set header
        header = new OutputHeader(numColumns, sortedColumns, bloomFilterColumns);
    }

    /**
     * Abstract builder used to set parameters to the input format's configuration in a fluent way.
     */
    protected static abstract class AbstractConfigBuilder<T> extends FileOutputFormat.AbstractConfigBuilder<T> {

        // --------------------------------------------------------------------

        /**
         * Creates a new builder for the given configuration.
         *
         * @param config The configuration into which the parameters will be written.
         */
        protected AbstractConfigBuilder(Configuration config) {
            super(config);
        }

        // --------------------------------------------------------------------

        /**
         * Adds a field of the record to be serialized to the output. The field at the given columnPositionInRecord will
         * be interpreted as the type represented by the given class. The types {@link Object#toString()} method
         * will be invoked to create a textual representation.
         *
         * @param type           The type of the field.
         * @param recordPosition The columnPositionInRecord in the record.
         * @return The builder itself.
         */
        public T field(Class<? extends Value> type, int recordPosition) {
            final int numYet = this.config.getInteger(NUM_COLUMNS_PARAMETER, 0);
            this.config.setClass(COLUMN_TYPE_PARAMETER_PREFIX + numYet, type);
            this.config.setInteger(RECORD_POSITION_PARAMETER_PREFIX + numYet, recordPosition);
            this.config.setInteger(NUM_COLUMNS_PARAMETER, numYet + 1);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Adds a field of the record to be serialized to the output. The field at the given columnPositionInRecord will
         * be interpreted as the type represented by the given class. The types {@link Object#toString()} method
         * will be invoked to create a textual representation.
         *
         * @param type           The type of the field.
         * @param recordPosition The columnPositionInRecord in the record.
         * @return The builder itself.
         */
        public T field(Class<? extends Value> type, int recordPosition, Class<? extends ICompressor> compressionType) {
            final int numYet = this.config.getInteger(NUM_COLUMNS_PARAMETER, 0);
            this.config.setClass(COLUMN_TYPE_PARAMETER_PREFIX + numYet, type);
            this.config.setInteger(RECORD_POSITION_PARAMETER_PREFIX + numYet, recordPosition);
            this.config.setClass(COLUMN_COMPRESSION_TYPE_PARAMETER_PREFIX + numYet, type);
            this.config.setInteger(NUM_COLUMNS_PARAMETER, numYet + 1);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets a new default compression used for column data.
         * <p/>
         * Default is {@link }
         *
         * @param compressionType
         * @return
         */
        public T defaultCompression(Class<? extends ICompressor> compressionType) {
            this.config.setClass(DEFAULT_COMPRESSION_TYPE_PARAMETER, compressionType);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets the leniency for the serializer. A lenient serializer simply skips missing fields and null
         * fields in the record, while a non lenient one throws an exception.
         *
         * @param lenient True, if the serializer should be lenient, false otherwise.
         * @return The builder itself.
         */
        public T lenient(boolean lenient) {
            this.config.setBoolean(LENIENT_PARSING, lenient);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Sets the row group size in bytes for this indexed pax file.
         * <p/>
         * Default setting is : 4 MB
         *
         * @param bytes the row group size in bytes.
         * @return
         */
        public T rowGroupSize(int bytes) {
            this.config.setInteger(ROW_GROUP_SIZE_PARAMETER, bytes);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Defines a column the rc file is sorted by.
         * <p/>
         * Default is no sorting.
         *
         * @param position
         * @param order
         * @return
         *
        public T sortBy(int position, Order order) {
        final int numYet = this.config.getInteger(SORT_COLUMNS_PARAMETER, 0);
        this.config.setInteger(SORT_COLUMN_POSITION_PREFIX + numYet, position);
        this.config.setString(SORT_COLUMN_ASC_PREFIX + numYet, order.toString());
        this.config.setInteger(SORT_COLUMNS_PARAMETER, numYet + 1);
         @SuppressWarnings("unchecked")
         T ret = (T) this;
         return ret;
         }
         */

        /**
         * Defines a column the rc file is sorted by.
         * <p/>
         * Default is no sorting.
         *
         * @param position
         * @return
         */
        public T sortBy(int position) {
            final int numYet = this.config.getInteger(SORT_COLUMNS_PARAMETER, 0);
            this.config.setInteger(SORT_COLUMN_POSITION_PREFIX + numYet, position);
            this.config.setString(SORT_COLUMN_ASC_PREFIX + numYet, Order.ASCENDING.toString());
            this.config.setInteger(SORT_COLUMNS_PARAMETER, numYet + 1);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Applies a bloom filter for the specified column.
         *
         * @param position the column position the bloom filter is applied.
         * @return
         */
        public T bloomFilter(int position) {
            final int numYet = this.config.getInteger(BLOOM_FILTER_COLUMN_PARAMETER, 0);
            this.config.setInteger(BLOOM_FILTER_COLUMN_POSITION_PREFIX + numYet, position);
            this.config.setInteger(BLOOM_FILTER_COLUMN_PARAMETER, numYet + 1);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }

        /**
         * Number of threads used to compress the column data.
         * <p/>
         * Default number is 1.
         *
         * @param numThreads the number of threads used for comression.
         * @return
         */
        public T threads(int numThreads) {
            this.config.setInteger(NUMBER_OF_THREADS_PARAMETER, numThreads);
            @SuppressWarnings("unchecked")
            T ret = (T) this;
            return ret;
        }
    }

    /**
     * A builder used to set parameters to the input format's configuration in a fluent way.
     */
    public static final class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder> {
        /**
         * Creates a new builder for the given configuration.
         *
         * @param targetConfig The configuration into which the parameters will be written.
         */
        protected ConfigBuilder(Configuration targetConfig) {
            super(targetConfig);
        }

    }

    /**
     * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
     * fashion.
     *
     * @return A config builder for setting parameters.
     */
    public static ConfigBuilder configureRCFormat(FileDataSink target) {
        return new ConfigBuilder(target.getParameters());
    }

}
