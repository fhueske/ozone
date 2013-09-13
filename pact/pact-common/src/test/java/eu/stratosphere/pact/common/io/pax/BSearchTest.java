package eu.stratosphere.pact.common.io.pax;

import java.io.File;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.ipc.Client;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.pax.selection.ISelection;
import eu.stratosphere.pact.common.io.pax.selection.LocalOperator;
import eu.stratosphere.pact.common.io.pax.selection.SelectionBuilder;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * @author Andreas Kunft
 */
public class BSearchTest {

    private static final String TXT_DUB = "file:///home/hduser/source/RCFile/src/test/resources/sort_Dub.txt";

    private static final String TXT_NO_DUB = "file:///home/hduser/source/RCFile/src/test/resources/sort_noDub.txt";

    private static final String TXT_UNIQUE = "file:///home/hduser/source/RCFile/src/test/resources/sort_unique.txt";

    private static final String RC_DUB = "file:///home/hduser/source/RCFile/src/test/resources/sort_Dub.rc";

    private static final String RC_NO_DUB = "file:///home/hduser/source/RCFile/src/test/resources/sort_noDub.rc";

    private static final String RC_UNIQUE = "file:///home/hduser/source/RCFile/src/test/resources/sort_unique.rc";

    private static final String CONFIG_DIR = "/home/hduser/stratosphere-0.2/conf";

    private static final String PATH_TO_JAR = "/home/hduser/source/stratosphere/out/artifacts/pax_format_jar/pax-format.jar";

    private FileDataSource source;

    private MapContract mapper;


    @BeforeClass
    public static void createRCFile() throws ProgramInvocationException, ErrorInPlanAssemblerException {
        File jar = new File(PATH_TO_JAR);
        // load configuration
        GlobalConfiguration.loadConfiguration(CONFIG_DIR);
        Configuration config = GlobalConfiguration.getConfiguration();
        Client client = new Client(config);

        String[] args = new String[]{TXT_DUB, RC_DUB};
        PactProgram prog = new PactProgram(jar, "de.tuberlin.pax.jobs.CreateBSearchSample", args);
        client.run(prog, true);

        args = new String[]{TXT_NO_DUB, RC_NO_DUB};
        prog = new PactProgram(jar, "de.tuberlin.pax.jobs.CreateBSearchSample", args);
        client.run(prog, true);

        args = new String[]{TXT_UNIQUE, RC_UNIQUE};
        prog = new PactProgram(jar, "de.tuberlin.pax.jobs.CreateBSearchSample", args);
        client.run(prog, true);
    }


    public void before(String rcFile) {
        source = new FileDataSource(IndexedPAXInputFormat.class, rcFile, "Sample in");
        IndexedPAXInputFormat.configureRCFormat(source)
                .field(PactInteger.class, 0)
                .field(PactInteger.class, 1);

        mapper = MapContract
                .builder(IdentityMap.class)
                .name("Map")
                .input(source)
                .build();
    }


    public static class IdentityMap extends MapStub {

        @Override
        public void map(final PactRecord record, final Collector<PactRecord> out) {
            out.collect(record);
        }
    }

    public static class IdentityReducer extends ReduceStub {

        @Override
        public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
            while (records.hasNext()) {
                out.collect(records.next());
            }
        }
    }

    @Test
    public void testEqual() {

        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(0));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        TestPlan testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(2))
                .add(new PactInteger(0), new PactInteger(3))
                .add(new PactInteger(0), new PactInteger(4))
                .add(new PactInteger(0), new PactInteger(5))
                .add(new PactInteger(0), new PactInteger(6))
                .add(new PactInteger(0), new PactInteger(7))
                .add(new PactInteger(0), new PactInteger(8))
                .add(new PactInteger(0), new PactInteger(9))
                .add(new PactInteger(0), new PactInteger(10));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(-10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(4));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(3));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(-10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(0));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(9));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(4));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(3));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(-10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(0));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactInteger(9));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();
    }

    @Test
    public void testGetOrGreater() {

        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(0));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        TestPlan testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(2))
                .add(new PactInteger(0), new PactInteger(3))
                .add(new PactInteger(0), new PactInteger(4))
                .add(new PactInteger(0), new PactInteger(5))
                .add(new PactInteger(0), new PactInteger(6))
                .add(new PactInteger(0), new PactInteger(7))
                .add(new PactInteger(0), new PactInteger(8))
                .add(new PactInteger(0), new PactInteger(9))
                .add(new PactInteger(0), new PactInteger(10));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(-10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(2))
                .add(new PactInteger(0), new PactInteger(3))
                .add(new PactInteger(0), new PactInteger(4))
                .add(new PactInteger(0), new PactInteger(5))
                .add(new PactInteger(0), new PactInteger(6))
                .add(new PactInteger(0), new PactInteger(7))
                .add(new PactInteger(0), new PactInteger(8))
                .add(new PactInteger(0), new PactInteger(9))
                .add(new PactInteger(0), new PactInteger(10));

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(4));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(3));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(-10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(0));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(9));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(4));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(3));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(-10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(0));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactInteger(9));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();
    }

    @Test
    public void testGreater() {

        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(0));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        TestPlan testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(-10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(2))
                .add(new PactInteger(0), new PactInteger(3))
                .add(new PactInteger(0), new PactInteger(4))
                .add(new PactInteger(0), new PactInteger(5))
                .add(new PactInteger(0), new PactInteger(6))
                .add(new PactInteger(0), new PactInteger(7))
                .add(new PactInteger(0), new PactInteger(8))
                .add(new PactInteger(0), new PactInteger(9))
                .add(new PactInteger(0), new PactInteger(10));

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(4));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(3));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(-10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(0));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(9));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(8));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(4));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(3));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(-10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(0));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(9));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.GREATER_THEN, 0, new PactInteger(8));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();
    }


    @Test
    public void testGetOrLower() {

        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(0));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        TestPlan testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(2))
                .add(new PactInteger(0), new PactInteger(3))
                .add(new PactInteger(0), new PactInteger(4))
                .add(new PactInteger(0), new PactInteger(5))
                .add(new PactInteger(0), new PactInteger(6))
                .add(new PactInteger(0), new PactInteger(7))
                .add(new PactInteger(0), new PactInteger(8))
                .add(new PactInteger(0), new PactInteger(9))
                .add(new PactInteger(0), new PactInteger(10));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(2))
                .add(new PactInteger(0), new PactInteger(3))
                .add(new PactInteger(0), new PactInteger(4))
                .add(new PactInteger(0), new PactInteger(5))
                .add(new PactInteger(0), new PactInteger(6))
                .add(new PactInteger(0), new PactInteger(7))
                .add(new PactInteger(0), new PactInteger(8))
                .add(new PactInteger(0), new PactInteger(9))
                .add(new PactInteger(0), new PactInteger(10));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(-10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(4));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(3));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(-10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(0));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(9));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(4));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(3));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(-10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(0));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_EQUAL_THEN, 0, new PactInteger(9));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();
    }

    @Test
    public void testLower() {

        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(0));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        TestPlan testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(2))
                .add(new PactInteger(0), new PactInteger(3))
                .add(new PactInteger(0), new PactInteger(4))
                .add(new PactInteger(0), new PactInteger(5))
                .add(new PactInteger(0), new PactInteger(6))
                .add(new PactInteger(0), new PactInteger(7))
                .add(new PactInteger(0), new PactInteger(8))
                .add(new PactInteger(0), new PactInteger(9))
                .add(new PactInteger(0), new PactInteger(10));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(-10));
        before(RC_UNIQUE);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(4));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(3));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81))
                .add(new PactInteger(9), new PactInteger(7))
                .add(new PactInteger(9), new PactInteger(91));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(-10));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(0));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(9));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(1), new PactInteger(31))
                .add(new PactInteger(1), new PactInteger(32))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(4), new PactInteger(41))
                .add(new PactInteger(4), new PactInteger(42))
                .add(new PactInteger(4), new PactInteger(43))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(8), new PactInteger(81));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(1));
        before(RC_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(0), new PactInteger(11));

        testPlan.run();

        //---------------------------------------------------------------------------------------------

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(4));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(3));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8))
                .add(new PactInteger(9), new PactInteger(7));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(-10));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(0));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class);

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(9));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1))
                .add(new PactInteger(1), new PactInteger(3))
                .add(new PactInteger(2), new PactInteger(5))
                .add(new PactInteger(4), new PactInteger(4))
                .add(new PactInteger(5), new PactInteger(2))
                .add(new PactInteger(6), new PactInteger(9))
                .add(new PactInteger(7), new PactInteger(10))
                .add(new PactInteger(8), new PactInteger(8));

        testPlan.run();

        selection = SelectionBuilder.buildSinglePredicate(LocalOperator.LESS_THEN, 0, new PactInteger(1));
        before(RC_NO_DUB);
        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactInteger.class, PactInteger.class)
                .add(new PactInteger(0), new PactInteger(1));

        testPlan.run();
    }

}
