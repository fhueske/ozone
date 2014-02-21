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

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.operators.translation.BinaryNodeTranslation;
import eu.stratosphere.api.java.operators.translation.PlanCogroupOperator;
import eu.stratosphere.api.java.operators.translation.PlanCrossOperator;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 */
public class CoGroupOperator<I1, I2, OUT> extends TwoInputUdfOperator<I1, I2, OUT, CoGroupOperator<I1, I2, OUT>> {
	private final CoGroupFunction<I1, I2, OUT> function;

	private final Keys<I1> keys1;
	private final Keys<I2> keys2;


	protected CoGroupOperator(DataSet<I1> input1, DataSet<I2> input2,
							  Keys<I1> keys1, Keys<I2> keys2,
							  CoGroupFunction<I1, I2, OUT> function,
							  TypeInformation<OUT> returnType)
	{
		super(input1, input2, returnType);

		this.function = function;

		if (keys1 == null || keys2 == null)
			throw new NullPointerException();
		
		this.keys1 = keys1;
		this.keys2 = keys2;
	}
	
	protected Keys<I1> getKeys1() {
		return this.keys1;
	}
	
	protected Keys<I2> getKeys2() {
		return this.keys2;
	}
	
	@Override
	protected BinaryNodeTranslation translateToDataFlow() {
		String name = getName() != null ? getName() : function.getClass().getName();
		return new BinaryNodeTranslation(new PlanCogroupOperator<I1, I2, OUT>(function, getKeys1().computeLogicalKeyPositions(), getKeys2().computeLogicalKeyPositions(), name, getInput1Type(), getInput2Type(), getResultType()));
	}

	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------
	
	public static final class CoGroupOperatorSets<I1, I2> {
		
		private final DataSet<I1> input1;
		private final DataSet<I2> input2;
		
		public CoGroupOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			if (input1 == null || input2 == null)
				throw new NullPointerException();
			
			this.input1 = input1;
			this.input2 = input2;
		}
		
		public CoGroupOperatorSetsPredicate where(int... fields) {
			return new CoGroupOperatorSetsPredicate(new Keys.FieldPositionKeys<I1>(fields, input1.getType()));
		}
		
		public <K> CoGroupOperatorSetsPredicate where(KeySelector<I1, K> keyExtractor) {
			return new CoGroupOperatorSetsPredicate(new Keys.SelectorFunctionKeys<I1, K>(keyExtractor, input1.getType()));
		}
		
		public CoGroupOperatorSetsPredicate where(String keyExpression) {
			return new CoGroupOperatorSetsPredicate(new Keys.ExpressionKeys<I1>(keyExpression, input1.getType()));
		}
	
		// ----------------------------------------------------------------------------------------
		
		public final class CoGroupOperatorSetsPredicate {
			
			private final Keys<I1> keys1;
			
			private CoGroupOperatorSetsPredicate(Keys<I1> keys1) {
				if (keys1 == null)
					throw new NullPointerException();
				
				if (keys1.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}
				
				this.keys1 = keys1;
			}
			
			
			public CoGroupOperatorWithoutFunction equalTo(int... fields) {
				return createCoGroupOperator(new Keys.FieldPositionKeys<I2>(fields, input2.getType()));
				
			}
			
			public <K> CoGroupOperatorWithoutFunction equalTo(KeySelector<I2, K> keyExtractor) {
				return createCoGroupOperator(new Keys.SelectorFunctionKeys<I2, K>(keyExtractor, input2.getType()));
			}
			
			public CoGroupOperatorWithoutFunction equalTo(String keyExpression) {
				return createCoGroupOperator(new Keys.ExpressionKeys<I2>(keyExpression, input2.getType()));
			}
			
			
			private CoGroupOperatorWithoutFunction createCoGroupOperator(Keys<I2> keys2) {
				if (keys2 == null)
					throw new NullPointerException();
				
				if (keys2.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}
				
				if (!keys1.areCompatibale(keys2)) {
					throw new InvalidProgramException("The pair of join keys are not compatible with each other.");
				}
				
				return new CoGroupOperatorWithoutFunction(keys2);
			}

			public final class CoGroupOperatorWithoutFunction {
				private final Keys<I2> keys2;

				private CoGroupOperatorWithoutFunction(Keys<I2> keys2) {
					if (keys2 == null)
						throw new NullPointerException();

					if (keys2.isEmpty()) {
						throw new InvalidProgramException("The join keys must not be empty.");
					}

					this.keys2 = keys2;
				}

				public <R> CoGroupOperator<I1, I2, R> with(CoGroupFunction<I1, I2, R> function) {
					TypeInformation<R> returnType = TypeExtractor.getCoGroupReturnTypes(function);
					return new CoGroupOperator<I1, I2, R>(input1, input2, keys1, keys2, function, returnType);
				}
			}
		}
	}
}
