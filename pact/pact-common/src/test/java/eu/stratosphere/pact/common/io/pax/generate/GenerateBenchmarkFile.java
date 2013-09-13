package eu.stratosphere.pact.common.io.pax.generate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * @author Andreas Kunft
 */
public class GenerateBenchmarkFile {

    private static final int CHUNK_SIZE = 500000;

    private static final int DIGITS = 4;

    private static final int RANGE_OFFSET = 1000;

    private static final int RANDOM_MAX_VALUE = 8999;

    private static final char DELIMITER = '|';

    private static final int DELIMITER_LENGTH = 1;

    private static final int LINEFEED_LENGTH = 1;

    private static final Random RANDOM = new Random(1234);

    private static final int UNIQUE_VALUE = RANDOM.nextInt(RANDOM_MAX_VALUE) + RANGE_OFFSET;

    private static float[] selectivity = {
            0.5f,
            0.1f,
            0.01f,
            0.001f
    };

    private static final List<List<Integer>> VALUES_PER_CHUNK = new ArrayList<List<Integer>>(selectivity.length);

    static {
        for (int i = 0; i < VALUES_PER_CHUNK.size(); i++) {
            VALUES_PER_CHUNK.add(new ArrayList<Integer>());
        }
    }

    private static int[][] chunksPerSelectivity;
    private static List<HashSet<Integer>> indexes;
    private static int lastChunkSize;
    private static int chunks;
    public static final int COLUMNS = 16;


    /**
     * Generates a test file of with 1, 10 or 100 GB of data.
     * <p/>
     * The columns are filled as follows:
     * <p/>
     * 1. fixed value 1
     * 2. random 0 / 1 (selectivity(1) = 0.5)
     * 3. random 0 / 1 (selectivity(1) = 0.1)
     * 4. random 0 / 1 (selectivity(1) = 0.01)
     * 5. random 0 / 1 (selectivity(1) = 0.001)
     * 6 - 16. uniformly distributed 0 / 1
     *
     * @param args
     */
    public static void main(String... args) {

        final int sizeInGB = ((args.length > 0) ? Integer.parseInt(args[0]) : 1
        );

        final long sizeInBytes = 1024L * 1024L * 1024L * sizeInGB;

        final int uniformColumns = COLUMNS - 1 - selectivity.length;

        final long valuesPerColumn = sizeInBytes / ((DIGITS + DELIMITER_LENGTH) * COLUMNS) + LINEFEED_LENGTH;

        long[] zero = new long[selectivity.length];
        long[] one = new long[selectivity.length];

        final Random[] random = new Random[uniformColumns];
        {
            for (int i = 0; i < uniformColumns; i++) {
                random[i] = new Random(i * 7);
            }
        }

        // generate chunks

        setValuesPerChunk(valuesPerColumn);

        final String filePath = "/home/hduser/benchmarkFiles/benchSample_" + sizeInGB + ".txt";

        final File file = new File(filePath);

        BufferedWriter writer = null;
        long rows = 0;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), Charset.forName("ASCII")));

            int lineNumber = 0;
            int currentChunk = 0;
            indexesForChunk(currentChunk);
            while (rows < valuesPerColumn) {

                if (lineNumber == CHUNK_SIZE && currentChunk != chunks - 1) {
                    currentChunk++;
                    indexesForChunk(currentChunk);
                    lineNumber = 0;
                }

                // first col with fix value
                StringBuilder line = new StringBuilder(UNIQUE_VALUE + "");
                line.append(DELIMITER);

                // selectivity
                int j = 0;
                for (HashSet<Integer> index : indexes) {
                    if (index.contains(lineNumber)) {
                        one[j]++;
                        line.append(UNIQUE_VALUE);
                    } else {
                        int val = RANDOM.nextInt(RANDOM_MAX_VALUE) + RANGE_OFFSET;
                        while (val == UNIQUE_VALUE) {
                            val = RANDOM.nextInt(RANDOM_MAX_VALUE) + RANGE_OFFSET;
                        }
                        zero[j]++;
                        line.append(val);
                    }
                    line.append(DELIMITER);
                    j++;
                }

                // uniform random between [RANGE_OFFSET, RANGE_OFFSET * RANDOM_MAX_VALUE[
                for (int i = 0; i < uniformColumns; i++) {
                    int number = random[i].nextInt(RANDOM_MAX_VALUE) + RANGE_OFFSET;
                    line.append(number);
                    line.append(DELIMITER);
                }

                line.append('\n');
                writer.write(line.toString());

                lineNumber++;
                rows++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                }
            }
        }

        for (int i = 0; i < selectivity.length; i++) {
            System.out.println(zero[i] + " " + one[i] + " " + (zero[i] + one[i]));
        }
        System.out.println(valuesPerColumn + ", " + rows);
    }

    private static void indexesForChunk(int currentChunk) {
        indexes = new ArrayList<HashSet<Integer>>(selectivity.length);

        int valuesInChunk = (currentChunk == chunks - 1) ? lastChunkSize : CHUNK_SIZE;

        for (int i = 0; i < selectivity.length; i++) {
            final int ones = chunksPerSelectivity[i][currentChunk];
            Random random = new Random();

            HashSet<Integer> uniqueValues = new HashSet<Integer>();
            while (uniqueValues.size() < ones) {

                uniqueValues.add(random.nextInt(valuesInChunk));
            }

            indexes.add(uniqueValues);
        }

    }

    private static void setValuesPerChunk(long valuesPerColumn) {
        chunks = (int) (valuesPerColumn / CHUNK_SIZE);
        if (valuesPerColumn % CHUNK_SIZE != 0) {
            lastChunkSize = (int) (CHUNK_SIZE + (valuesPerColumn % CHUNK_SIZE));
        }

        chunksPerSelectivity = new int[selectivity.length][];
        int i = 0;
        for (float selectivity : GenerateBenchmarkFile.selectivity) {
            final int uniqueValues = (int) (valuesPerColumn * selectivity);
            Random random = new Random();

            int numSet = 0;
            int[] onesPerChunk = new int[chunks];
            while (numSet < uniqueValues) {
                int index = random.nextInt(chunks);
                // last chunk
                if ((index == chunks - 1 && onesPerChunk[index] < lastChunkSize) || (onesPerChunk[index] < CHUNK_SIZE)) {
                    onesPerChunk[index]++;
                    numSet++;
                }
            }
            chunksPerSelectivity[i] = onesPerChunk;
            i++;
        }
    }
}
