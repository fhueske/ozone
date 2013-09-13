package eu.stratosphere.pact.common.io.pax.generate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;

/**
 * @author Andreas Kunft
 */
public class GenerateIntOnlyTestFile {

    public static class CountWriter extends BufferedWriter {

        public long written;

        public CountWriter(Writer writer) {
            super(writer);
        }

        @Override
        public void write(String s) throws IOException {
            written += (s.length() + 1) * 8;
            super.write(s);    //To change body of overridden methods use File | Settings | File Templates.
        }
    }

    public static void main(String... args) throws IOException {

        Random random = new Random();

        int columns = 10;

        String filePath = "/Users/buzl/dev/test/in/intSample.txt";

        File file = new File(filePath);

        CountWriter writer = new CountWriter(new OutputStreamWriter(new FileOutputStream(file)));

        int index = 1;
        long bound = (long) 4 * 1024 * 1024 * 1024 * 10;
        while (writer.written <= bound) {
            StringBuilder line = new StringBuilder("" + index++ + "|");
            for (int i = 0; i < columns - 1; i++) {
                line.append(random.nextInt() + "|");
            }
            writer.write(line.toString() + "\n");
        }
        
        writer.close();
    }
}
