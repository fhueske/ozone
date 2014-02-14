/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.typeutils.TypeInformation;


public class DataSink<T> {
	
	private final OutputFormat<T> format;
	
	private final ExecutionEnvironment context;
	
	private final TypeInformation<T> type;
	
	
	public DataSink(ExecutionEnvironment context, OutputFormat<T> format, TypeInformation<T> type) {
		if (format == null)
			throw new IllegalArgumentException("The output format must not be null.");
		if (type == null)
			throw new IllegalArgumentException("The input type information must not be null.");
		if (context == null)
			throw new IllegalArgumentException("The execution environment must not be null.");
		
		
		this.format = format;
		this.context = context;
		this.type = type;
	}

	
	public OutputFormat<T> getFormat() {
		return format;
	}
	
	public ExecutionEnvironment getContext() {
		return context;
	}
	
	public TypeInformation<T> getType() {
		return type;
	}
	
	// --------------------------------------------------------------------------------------------
	
	
	
	// --------------------------------------------------------------------------------------------

}