package eu.stratosphere.pact.common.io.pax;

import java.io.File;
import java.util.Iterator;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.ipc.Client;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.pax.selection.ISelection;
import eu.stratosphere.pact.common.io.pax.selection.LocalOperator;
import eu.stratosphere.pact.common.io.pax.selection.LogicalOperator;
import eu.stratosphere.pact.common.io.pax.selection.SelectionBuilder;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * @author Andreas Kunft
 */
public class SelectionWithBloomFilterTest {

    private static final String TXT_FILE = "file:///home/hduser/source/RCFile/src/test/resources/sample.txt";

    private static final String RC_FILE = "file:///home/hduser/source/RCFile/src/test/resources/sample_bloom.rc";

    private static final String CONFIG_DIR = "/home/hduser/stratosphere-0.2/conf";

    private static final String PATH_TO_JAR = "/home/hduser/source/stratosphere/out/artifacts/pax_format_jar/pax-format.jar";

    private FileDataSource source;

    private MapContract mapper;
    private ReduceContract reducer;


    @BeforeClass
    public static void createRCFile() throws ProgramInvocationException, ErrorInPlanAssemblerException {
        File jar = new File(PATH_TO_JAR);
        // load configuration
        GlobalConfiguration.loadConfiguration(CONFIG_DIR);
        Configuration config = GlobalConfiguration.getConfiguration();
        String[] args = new String[]{TXT_FILE, RC_FILE, "2"};
        PactProgram prog = new PactProgram(jar, "de.tuberlin.pax.jobs.CreateTestSample", args);
        Client client = new Client(config);
        client.run(prog, true);
    }


    @Before
    public void beforeClass() {
        source = new FileDataSource(IndexedPAXInputFormat.class, RC_FILE, "Sample in");
        IndexedPAXInputFormat.configureRCFormat(source)
                .field(PactLong.class, 0)
                .field(PactString.class, 1)
                .field(PactString.class, 2);

        mapper = MapContract
                .builder(IdentityMap.class)
                .name("Map")
                .input(source)
                .build();

        reducer = ReduceContract
                .builder(IdentityReducer.class)
                .name("Reduce")
                .input(mapper)
                .keyField(PactLong.class, 0)
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
    public void testSelectionSinglePredicate() {

        ISelection selection = SelectionBuilder.buildSinglePredicate(LocalOperator.EQUAL, 0, new PactLong(1));

        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        TestPlan testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactLong.class, PactString.class, PactString.class)
                // 1|copy2|7asfadf23524tsgasdg
                .add(new PactLong(1), new PactString("copy2"), new PactString("7asfadf23524tsgasdg"));
        testPlan.run();
    }

    @Test
    public void testSelectionSimple() {
        SelectionBuilder selection = SelectionBuilder.create()
                .startComposition(LogicalOperator.AND)
                .predicate(LocalOperator.GREATER_EQUAL_THEN, 0, new PactLong(6))
                .predicate(LocalOperator.EQUAL, 1, new PactString("copy1"))
                .startComposition(LogicalOperator.OR)
                .predicate(LocalOperator.EQUAL, 2, new PactString("4asfadf23524tsgasdsdfasdfasdfasfdasdfg"))
                .predicate(LocalOperator.EQUAL, 2, new PactString("4asfadf23524tsgasdg"))
                .endComposition()
                .endComposition();

        IndexedPAXInputFormat.configureRCFormat(source).selection(selection.build());

        TestPlan testPlan = new TestPlan(mapper);
        testPlan.getExpectedOutput(PactLong.class, PactString.class, PactString.class)
                // 6|copy1|4asfadf23524tsgasdg
                // 6|copy1|4asfadf23524tsgasdsdfasdfasdfasfdasdfg
                .add(new PactLong(6), new PactString("copy1"), new PactString("4asfadf23524tsgasdg"))
                .add(new PactLong(6), new PactString("copy1"), new PactString("4asfadf23524tsgasdsdfasdfasdfasfdasdfg"));
        testPlan.run();
    }

    @Test
    public void testSelectionCreationComplex() {

        ISelection selection = SelectionBuilder.create()
                .startComposition(LogicalOperator.OR)
                .predicate(LocalOperator.EQUAL, 0, new PactLong(1))
                .predicate(LocalOperator.BETWEEN, 0, new PactLong(5), new PactLong(8))
                .startComposition(LogicalOperator.AND)
                .predicate(LocalOperator.EQUAL, 2, new PactString("2asdf"))
                .startComposition(LogicalOperator.OR)
                .predicate(LocalOperator.EQUAL, 0, new PactLong(3))
                .predicate(LocalOperator.BETWEEN, 0, new PactLong(0), new PactLong(2))
                .endComposition()
                .endComposition()
                .startComposition(LogicalOperator.AND)
                .predicate(LocalOperator.EQUAL, 0, new PactLong(26))
                .predicate(LocalOperator.EQUAL, 1, new PactString("rosssssw3"))
                .endComposition()
                .endComposition()
                .build();

        IndexedPAXInputFormat.configureRCFormat(source).selection(selection);

        TestPlan testPlan = new TestPlan(reducer);
        testPlan.getExpectedOutput(PactLong.class, PactString.class, PactString.class)
                //        0|copy0|2asdf
                //        1|copy2|7asfadf23524tsgasdg
                //        3|row4|2asdf
                //        5|copssy|3agarqwet25
                //        6|copy1|4asfadf23524tsgasdsdfasdfasdfasfdasdfg
                //        7|row5|5asdfasdfawet253taw
                //        8|copyax|7asfadf23524tsgasdg
                //        6|copy2|4asfadf23524tsgasdg
                //        6|copy1|4asfadf23524tsgasdg
                //        26|rosssssw3|6asdf
                //        6|copy1|4as
                .add(new PactLong(0), new PactString("copy0"), new PactString("2asdf"))
                .add(new PactLong(1), new PactString("copy2"), new PactString("7asfadf23524tsgasdg"))
                .add(new PactLong(3), new PactString("row4"), new PactString("2asdf"))
                .add(new PactLong(5), new PactString("copssy"), new PactString("3agarqwet25"))
                .add(new PactLong(6), new PactString("copy1"), new PactString("4asfadf23524tsgasdsdfasdfasdfasfdasdfg"))
                .add(new PactLong(6), new PactString("copy2"), new PactString("4asfadf23524tsgasdg"))
                .add(new PactLong(6), new PactString("copy1"), new PactString("4asfadf23524tsgasdg"))
                .add(new PactLong(6), new PactString("copy1"), new PactString("4as"))
                .add(new PactLong(7), new PactString("row5"), new PactString("5asdfasdfawet253taw"))
                .add(new PactLong(8), new PactString("copyax"), new PactString("7asfadf23524tsgasdg"))
                .add(new PactLong(26), new PactString("rosssssw3"), new PactString("6asdf"));

        testPlan.run();
    }

}
