package eu.stratosphere.api.java.operators.translation;

import java.util.Iterator;

import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.common.functions.GenericCrosser;
import eu.stratosphere.api.common.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Reference;

public class PlanCogroupOperator<IN1, IN2, OUT> 
	extends CoGroupOperatorBase<GenericCoGrouper<Reference<IN1>, Reference<IN2>, Reference<OUT>>>
	implements BinaryJavaPlanNode<IN1, IN2, OUT> {
	
	private final TypeInformation<IN1> inType1;
	private final TypeInformation<IN2> inType2;
	private final TypeInformation<OUT> outType;

	public PlanCogroupOperator(
			UserCodeWrapper<GenericCoGrouper<Reference<IN1>, Reference<IN2>, Reference<OUT>>> udf,
			int[] keyPositions1, int[] keyPositions2, String name, TypeInformation<IN1> inType1, TypeInformation<IN2> inType2, TypeInformation<OUT> outType) {
		super(udf, keyPositions1, keyPositions2, name);
		
		this.inType1 = inType1;
		this.inType2 = inType2;
		this.outType = outType;
	}
	
	public static final class ReferenceWrappingCogrouper<IN1, IN2, OUT> 
	extends WrappingFunction<CoGroupFunction<IN1, IN2, OUT>>
	implements GenericCoGrouper<Reference<IN1>, Reference<IN2>, Reference<OUT>>
	{
		
		private static final long serialVersionUID = 1L;
		
		private final Reference<OUT> ref = new Reference<OUT>();
		
		protected ReferenceWrappingCogrouper(
				CoGroupFunction<IN1, IN2, OUT> wrappedFunction) {
			super(wrappedFunction);
		}
		
		@Override
		public void coGroup(final Iterator<Reference<IN1>> records1,
				final Iterator<Reference<IN2>> records2, final Collector<Reference<OUT>> out)
				throws Exception {
			
			Collector<OUT> refOutCollector = new Collector<OUT>() {

				@Override
				public void collect(OUT record) {
					out.collect(new Reference<OUT>(record));
				}
				
				@Override
				public void close() {
					out.close();
				}
			};
			
			this.wrappedFunction.coGroup(new UnwrappingIterator<IN1>(records1), new UnwrappingIterator<IN2>(records2), refOutCollector);
			
		}


		@Override
		public void combineFirst(Iterator<Reference<IN1>> records,
				Collector<Reference<IN1>> out) throws Exception {
			// TODO Auto-generated method stub
			
		}


		@Override
		public void combineSecond(Iterator<Reference<IN2>> records,
				Collector<Reference<IN2>> out) throws Exception {
			// TODO Auto-generated method stub
			
		}
	}

	@Override
	public TypeInformation<OUT> getReturnType() {
		return this.outType;
	}


	@Override
	public TypeInformation<IN1> getInputType1() {
		return this.inType1;
	}


	@Override
	public TypeInformation<IN2> getInputType2() {
		return this.inType2;
	}
	
	public static class UnwrappingCollector<T> implements Collector<T> {
		
		Collector<Reference<T>> outerCollector;

		public UnwrappingCollector(Collector<Reference<T>> outerCollector) {
			this.outerCollector = outerCollector;
		}
		
		@Override
		public void collect(T record) {
			this.outerCollector.collect(new Reference(record));
		}

		@Override
		public void close() {
			this.outerCollector.close();
		}
	}
	
	public static class UnwrappingIterator<T> implements Iterator<T> {

		private Iterator<? extends Reference<T>> outerIterator;
		
		public UnwrappingIterator(Iterator<? extends Reference<T>> outerIterator) {
			this.outerIterator = outerIterator;
		}
		
		@Override
		public boolean hasNext() {
			return outerIterator.hasNext();
		}

		@Override
		public T next() {
			return outerIterator.next().ref;
		}

		@Override
		public void remove() {
			this.outerIterator.remove();
		}
		
	}

}
