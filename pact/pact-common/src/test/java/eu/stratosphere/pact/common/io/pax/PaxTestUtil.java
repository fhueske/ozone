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

import java.io.File;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

public class PaxTestUtil {

	public static void addPredToInputConfig(Configuration config, int predId, int groupId, String op, int pos, Class<? extends Key> type, String val) {
    	config.setInteger(IndexedPAXInputFormat.PREDICATE_SELECTION_GROUP_PARAMETER_PREFIX + predId, groupId);
    	config.setString(IndexedPAXInputFormat.SELECTION_OPERATOR_PARAMETER_PREFIX + predId, op);
    	config.setInteger(IndexedPAXInputFormat.SELECTION_POSITION_PARAMETER_PREFIX + predId, pos);
    	config.setClass(IndexedPAXInputFormat.SELECTION_TYPE_PARAMETER_PREFIX + predId, type);
    	config.setString(IndexedPAXInputFormat.SELECTION_VALUE_PARAMETER_PREFIX + predId, val);
    }
    
	public static void addColToInputConfig(Configuration config, int colId, int pos, Class<? extends Key> type) {
    	config.setInteger(IndexedPAXInputFormat.COLUMN_POSITION_PARAMETER_PREFIX + colId , pos);
    	config.setClass(IndexedPAXInputFormat.COLUMN_TYPE_PARAMETER_PREFIX + colId, type);
    }
    
    public static void addColToOutputConfig(Configuration config, int colId, int pos, Class<? extends Key> type) {
    	config.setInteger(IndexedPAXOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + colId , pos);
    	config.setClass(IndexedPAXOutputFormat.COLUMN_TYPE_PARAMETER_PREFIX + colId, type);
    }
    
    public static File generateTestData(String[] types, boolean[] sorting, boolean[] bloomFilters, Object[]... data) throws IOException {
    	
    	if(data == null || types == null) {
    		throw new RuntimeException("types and data are required");
    	}
    	if(types.length == 0) {
    		throw new RuntimeException("At least one data type required");
    	}
    	if(data.length != types.length) {
    		throw new RuntimeException("As may data arrays required as types");
    	}
    	if(sorting != null && sorting.length != types.length) {
    		throw new RuntimeException("Sorting flags for each column required");
    	}
    	if(bloomFilters != null && bloomFilters.length != types.length) {
    		throw new RuntimeException("Bloomfilter flags for each column required");
    	}
    	
    	File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();
		
    	IndexedPAXOutputFormat outFormat = new IndexedPAXOutputFormat();
    	Key[] colVals = new Key[types.length];
    	PactRecord outRecord = new PactRecord(types.length);
    	
    	int numRecords = -1;

    	Configuration parameters = new Configuration();
    	parameters.setString(FileOutputFormat.FILE_PARAMETER_KEY, "file://"+tempFile.getAbsolutePath());
		// define output
    	parameters.setBoolean(IndexedPAXOutputFormat.LENIENT_PARSING, true);
		parameters.setInteger(IndexedPAXOutputFormat.NUM_COLUMNS_PARAMETER, types.length);
		for(int i=0;i<types.length;i++) {
			if(types[i].equals("INT")) {
				addColToOutputConfig(parameters, i, i, PactInteger.class);
				colVals[i] = new PactInteger();
			} else if(types[i].equals("LONG")) {
				addColToOutputConfig(parameters, i, i, PactLong.class);
				colVals[i] = new PactLong();
			} else if(types[i].equals("DOUBLE")) {
				addColToOutputConfig(parameters, i, i, PactDouble.class);
				colVals[i] = new PactDouble();
			} else if(types[i].equals("STRING")) {
				addColToOutputConfig(parameters, i, i, PactString.class);
				colVals[i] = new PactString();
			} else {
				throw new RuntimeException("Unsupported Test Data Type");
			}
			if(numRecords == -1) {
				numRecords = data[i].length;
			} else {
				if (numRecords != data[i].length) {
					throw new RuntimeException("All data columns must hold the same number of values");
				}
			}
		}
		if(sorting != null) {
			
			int sortingCnt = 0;
			for(int i=0;i<types.length;i++) {
				if(sorting[i]) {
					parameters.setInteger(IndexedPAXOutputFormat.SORT_COLUMN_POSITION_PREFIX+sortingCnt, i);
					parameters.setString(IndexedPAXOutputFormat.SORT_COLUMN_ASC_PREFIX+sortingCnt, Order.ASCENDING.toString());
					sortingCnt++;
				}
			}
			parameters.setInteger(IndexedPAXOutputFormat.SORT_COLUMNS_PARAMETER, sortingCnt);
		}
		if(bloomFilters != null) {
			
			int bloomFilterCnt = 0;
			for(int i=0;i<types.length;i++) {
				if(bloomFilters[i]) {
					parameters.setInteger(IndexedPAXOutputFormat.BLOOM_FILTER_COLUMN_POSITION_PREFIX+bloomFilterCnt, i);
					bloomFilterCnt++;
				}
			}
			parameters.setInteger(IndexedPAXOutputFormat.BLOOM_FILTER_COLUMN_PARAMETER, bloomFilterCnt);
		}
	
		outFormat.configure(parameters);
		outFormat.open(0);
    	
		for(int i=0;i<numRecords;i++) {
			for(int j=0;j<data.length;j++) {
				if(types[j].equals("INT")) {
					((PactInteger)colVals[j]).setValue(((Integer)(data[j][i])).intValue());
				} else if(types[j].equals("LONG")) {
					((PactLong)colVals[j]).setValue(((Long)(data[j][i])).longValue());
				} else if(types[j].equals("DOUBLE")) {
					((PactDouble)colVals[j]).setValue(((Double)(data[j][i])).doubleValue());
				} else if(types[j].equals("STRING")) {
					((PactString)colVals[j]).setValue(((String)(data[j][i])));
				}
				outRecord.setField(j, colVals[j]);
			}
			outFormat.writeRecord(outRecord);
		}
		outFormat.close();
		
		return tempFile; 
    }
	
}
