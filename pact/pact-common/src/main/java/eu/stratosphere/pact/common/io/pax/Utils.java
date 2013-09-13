package eu.stratosphere.pact.common.io.pax;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Class with static utility methods.
 *
 * @author Andreas Kunft
 */
public class Utils {

    /**
     * Sets the value in the supplied type by parsing the value String.
     * <p/>
     * Currently only double, int, long and null are supported.
     *
     * @param type  the pact type
     * @param value the string representation of the value
     * @return true if the operation is valid for the supplied type
     */
    public static boolean setPactValueFromString(Value type, String value) {
        if (type instanceof PactDouble) {
            ((PactDouble) type).setValue(Double.parseDouble(value));
            return true;
        } else if (type instanceof PactInteger) {
            ((PactInteger) type).setValue(Integer.parseInt(value));
            return true;
        } else if (type instanceof PactLong) {
            ((PactLong) type).setValue(Long.parseLong(value));
            return true;
        } else if (type instanceof PactNull) {
            return true;
        } else if (type instanceof PactString) {
            ((PactString) type).setValue(value);
            return true;
        }
        return false;
    }


    /**
     * Returns the number of bytes the value type has on Disk.
     * <p/>
     * THIS METHOD EXPECTES THE VALUE TO BE WRITTEN VIA A DATAOUTPUTSTREAM.
     *
     * @param value
     * @return
     */
    public static int getValueBytes(Value value) {
        if (value instanceof PactDouble) {
            return 8;
        } else if (value instanceof PactInteger) {
            return 4;
        } else if (value instanceof PactLong) {
            return 8;
        } else if (value instanceof PactNull) {
            return 1;
        } else if (value instanceof PactString) {
            return ((PactString) value).length();
        } else {
            throw new RuntimeException("Unsupported value type");
        }
    }

    public static void writeInt(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    public static int readInt(InputStream in) throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public static byte getByteForValueClass(Class<? extends Value> value) {
        if (value == PactDouble.class) {
            return 0x01;
        } else if (value == PactInteger.class) {
            return 0x02;
        } else if (value == PactLong.class) {
            return 0x03;
        } else if (value == PactString.class) {
            return 0x04;
        } else {
            throw new RuntimeException("Unsupported value type");
        }
    }

    public static Class<? extends Value> getValueClassForByte(byte value) {
        if (value == 0x01) {
            return PactDouble.class;
        } else if (value == 0x02) {
            return PactInteger.class;
        } else if (value == 0x03) {
            return PactLong.class;
        } else if (value == 0x04) {
            return PactString.class;
        } else {
            throw new RuntimeException("Unsupported value type");
        }
    }

    public static Value copy(Value value) {
        if (value instanceof PactDouble) {
            return new PactDouble(((PactDouble) value).getValue());
        } else if (value instanceof PactInteger) {
            return new PactInteger(((PactInteger) value).getValue());
        } else if (value instanceof PactLong) {
            return new PactLong(((PactLong) value).getValue());
        } else if (value instanceof PactString) {
            return new PactString(((PactString) value).getValue());
        } else {
            throw new RuntimeException("Unsupported value type");
        }
    }


}
