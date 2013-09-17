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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class BSearchTest {

	private final Integer[] dubSetCol0 = {0, 0, 5, 1, 1, 1, 4, 4, 4, 4, 2, 9, 9, 8, 8, 6, 7};
	private final Integer[] dubSetCol1 = {1,11, 2, 3,31,32, 4,41,42,43, 5, 7,91, 8,81, 9,10};
	
	private final Integer[] uniqSetCol0 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	private final Integer[] uniqSetCol1 = {1, 2, 3, 4, 5, 6, 7, 8, 9,10};
	
	private final Integer[] noDubSetCol0 = {0, 5, 1, 4, 2, 9, 8, 6, 7};
	private final Integer[] noDubSetCol1 = {1, 2, 3, 4, 5, 7, 8, 9,10};
	
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
    public void testEqualUnique() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			uniqSetCol0, uniqSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 10 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(6, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
					
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// ------
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    
    @Test
    public void testEqualDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			dubSetCol0, dubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 4 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 2 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// 2 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testEqualNoDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			noDubSetCol0, noDubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// one record expected
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 1 record expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "EQUAL", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// one record expected
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGetOrGreaterUnique() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			uniqSetCol0, uniqSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 10 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(6, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
					
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// 10 records output
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(6, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

    	} catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGetOrGreaterDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			dubSetCol0, dubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 11 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// 11 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// 17 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
            
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 17 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
            
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// 2 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGetOrGreaterNoDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			noDubSetCol0, noDubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// seven records expected
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// expect 7 records
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// 9 records
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 10 record expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_EQUAL_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// one record expected
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGreaterUnique() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			uniqSetCol0, uniqSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// no records expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// 10 records output
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(6, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

    	} catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGreaterDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			dubSetCol0, dubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 7 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// 11 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// 17 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
            
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 15 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
            
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// no records expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGreaterNoDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			noDubSetCol0, noDubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 6 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// expect 7 records
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// 10 records
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 9 record expected
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "GREATER_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// no record expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGetOrLessUnique() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			uniqSetCol0, uniqSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 10 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(6, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
					
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			/*
			while(format.nextRecord(record)) {
				System.out.println(record.getField(0, PactInteger.class).getValue()+" : "+record.getField(1, PactInteger.class).getValue());
			}
			System.out.println("------");
			*/
			
			// 10 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(6, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
					
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// no records expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();

			// ------
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    
    @Test
    public void testGetOrLessDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			dubSetCol0, dubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 10 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// 6 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
            
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			/*
			while(format.nextRecord(record)) {
				System.out.println(record.getField(0, PactInteger.class).getValue()+" : "+record.getField(1, PactInteger.class).getValue());
			}
			System.out.println("------");
			*/
			
			// 17 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 2 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// 17 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testGetOrLessNoDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			noDubSetCol0, noDubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 4 records
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// 3 records
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			/*
			while(format.nextRecord(record)) {
				System.out.println(record.getField(0, PactInteger.class).getValue()+" : "+record.getField(1, PactInteger.class).getValue());
			}
			System.out.println("------");
			*/
			
			// 9 records
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// 1 record expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_EQUAL_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// 9 records
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testLessUnique() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			uniqSetCol0, uniqSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// no records expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// 10 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(6, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());

			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// no records expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
		
			format.close();

			// ------
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    
    @Test
    public void testLessDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			dubSetCol0, dubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 6 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
            
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());

			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// 6 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
            
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// 17 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(91, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// no records expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// 15 records expected
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(81, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(41, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(42, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(43, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(31, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(32, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(11, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
    @Test
    public void testLessNoDublicates() {

    	try {
    	
	    	tempFile = PaxTestUtil.generateTestData(
	    			new String[]{"INT","INT"},
	    			new boolean[]{true, false}, null, 
	    			noDubSetCol0, noDubSetCol1);
	    	final FileInputSplit split = new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
	    	
	    	Configuration parameters = new Configuration();
			parameters.setString(IndexedPAXInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
			// define output
			parameters.setInteger(IndexedPAXInputFormat.COLUMN_NUM_PARAMETER, 2);
			PaxTestUtil.addColToInputConfig(parameters, 0, 0, PactInteger.class);
			PaxTestUtil.addColToInputConfig(parameters, 1, 1, PactInteger.class);
			// define selection
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_GROUP_PARAMETER, 0);
			parameters.setBoolean(IndexedPAXInputFormat.SELECTION_SINGLE_PREDICATE, true);
			parameters.setInteger(IndexedPAXInputFormat.SELECTION_NUM_PARAMETER, 1);
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "4");
			
			format.configure(parameters);
			format.open(split);
			
			PactRecord record = new PactRecord();
			
			// 3 records
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "3");
			
			format.configure(parameters);
			format.open(split);
			
			// 3 records
			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());

			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "10");
			
			format.configure(parameters);
			format.open(split);
			
			// 9 records
			assertTrue(format.nextRecord(record));
			assertEquals(9, record.getField(0, PactInteger.class).getValue());
			assertEquals(7, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// ------
			
			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "-10");
			
			format.configure(parameters);
			format.open(split);
			
			// expect no output
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();

			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "0");
			
			format.configure(parameters);
			format.open(split);
			
			// no record expected
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
			// -----

			// overwrite predicate
			PaxTestUtil.addPredToInputConfig(parameters, 0, 0, "LESS_THAN", 0, PactInteger.class, "9");
			
			format.configure(parameters);
			format.open(split);
			
			// 8 records
			assertTrue(format.nextRecord(record));
			assertEquals(8, record.getField(0, PactInteger.class).getValue());
			assertEquals(8, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(7, record.getField(0, PactInteger.class).getValue());
			assertEquals(10, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(6, record.getField(0, PactInteger.class).getValue());
			assertEquals(9, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(5, record.getField(0, PactInteger.class).getValue());
			assertEquals(2, record.getField(1, PactInteger.class).getValue());
			
			assertTrue(format.nextRecord(record));
			assertEquals(4, record.getField(0, PactInteger.class).getValue());
			assertEquals(4, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(2, record.getField(0, PactInteger.class).getValue());
			assertEquals(5, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(1, record.getField(0, PactInteger.class).getValue());
			assertEquals(3, record.getField(1, PactInteger.class).getValue());

			assertTrue(format.nextRecord(record));
			assertEquals(0, record.getField(0, PactInteger.class).getValue());
			assertEquals(1, record.getField(1, PactInteger.class).getValue());
			
			assertFalse(format.nextRecord(record));
			assertTrue(format.reachedEnd());
			
			format.close();
			
    	}
		catch (Exception ex) {
			Assert.fail("Test failed due to a " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
    }
    
}
