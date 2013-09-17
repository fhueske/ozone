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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

public class SelectionWithBloomFilterTest {

	// test data
    private Long[] 		testCol0 = {15l,0l,1l,3l,4l,5l,6l,7l,8l,9l,10l,11l,6l,12l,13l,14l,20l,22l,24l,6l,26l,6l};
    private String[] 	testCol1 = {"copy","copy0","copy2","row4","row5","copssy","copy1","row5",
    							 "copyax","row6","row1","row2","copy2","copy","caaaaopy","row5",
    							 "rosssssw3","rosssssw3","rosssssw3","copy1","rosssssw3","copy1"};
    private String[] 	testCol2 = {"7asfadf23524tsgasdg","2asdf","7asfadf23524tsgasdg","2asdf","2asdf",
    							 "3agarqwet25","4asfadf23524tsgasdsdfasdfasdfasfdasdfg","5asdfasdfawet253taw",
    							 "7asfadf23524tsgasdg","6asdf","1asfasf","2asdf","4asfadf23524tsgasdg",
    							 "3agarqwet25","4asfadf23524tsgasdg","5asdfasdfawet253taw","6asdf",
    							 "6asdf","6asdf","4asfadf23524tsgasdg","6asdf","4as"};
    
    @Mock
	protected Configuration config;
	
	protected File tempFile;
	
	private final IndexedPAXInputFormat format = new IndexedPAXInputFormat();
	
	// --------------------------------------------------------------------------------------------
	
	@Before
	public void setup() {
		initMocks(this);
	}
	
	@After
	public void setdown() throws Exception {
		if (this.format != null) {
			this.format.close();
		}
		if (this.tempFile != null) {
			this.tempFile.delete();
		}
	}
    
    @Test
	public void testSinglePred() throws IOException {
		try {
			
			tempFile = PaxTestUtil.generateTestData(
					new String[]{"LONG","STRING","STRING"},
					null, new boolean[]{true, true, true},
					testCol0, testCol1, testCol2);
			final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
			
			final Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 3);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactLong.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactString.class);
			PaxTestUtil.addColToInputConfig(parameters, 2, 2, PactString.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactLong.class, "1");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactLong.class).getValue());
			assertEquals("copy2", record.getField(1, PactString.class).getValue());
			assertEquals("7asfadf23524tsgasdg", record.getField(2, PactString.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}

    @Test
	public void testMultiPred() throws IOException {
		try {
			
			tempFile = PaxTestUtil.generateTestData(
					new String[]{"LONG","STRING","STRING"},
					null, new boolean[]{true, true, true},
					testCol0, testCol1, testCol2);
			final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
			
			final Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 3);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactLong.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactString.class);
			PaxTestUtil.addColToInputConfig(parameters, 2, 2, PactString.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 4);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 1);
			// AND
			parameters.setString(IndexedPAXInputFormat.SELECTION_GROUP_LOGICAL_OPERATOR_PREFIX + 0, "AND");
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactLong.class, "6");
			PaxTestUtil.addPredToInputConfig(parameters, 1, 0, "EQUAL", 1, PactString.class, "copy1");
				// OR 
				parameters.setString(IndexedPAXInputFormat.SELECTION_GROUP_LOGICAL_OPERATOR_PREFIX + 1, "OR");
				PaxTestUtil.addPredToInputConfig(parameters, 2, 1, "EQUAL", 2, PactString.class, "4asfadf23524tsgasdsdfasdfasdfasfdasdfg");
				PaxTestUtil.addPredToInputConfig(parameters, 3, 1, "EQUAL", 2, PactString.class, "4asfadf23524tsgasdg");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();

			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactLong.class).getValue());
			assertEquals("copy1", record.getField(1, PactString.class).getValue());
			assertEquals("4asfadf23524tsgasdsdfasdfasdfasfdasdfg", record.getField(2, PactString.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactLong.class).getValue());
			assertEquals("copy1", record.getField(1, PactString.class).getValue());
			assertEquals("4asfadf23524tsgasdg", record.getField(2, PactString.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
		}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}

}
