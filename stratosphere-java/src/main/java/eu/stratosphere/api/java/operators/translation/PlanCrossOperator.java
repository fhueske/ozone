package eu.stratosphere.api.java.operators.translation;

import eu.stratosphere.api.common.functions.GenericCrosser;
import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.base.CrossOperatorBase;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.operators.translation.PlanMapOperator.ReferenceWrappingMapper;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Reference;

public class PlanCrossOperator<IN1, IN2, OUT> extends CrossOperatorBase<GenericCrosser<Reference<IN1>, Reference<IN2>, Reference<OUT>>> {

	public PlanCrossOperator(
			CrossFunction<IN1, IN2, OUT> udf,
			String name) {
		super(new ReferenceWrappingCrosser<IN1, IN2, OUT>(udf), name);
		// TODO Auto-generated constructor stub
	}
	
	
	public static final class ReferenceWrappingCrosser<IN1, IN2, OUT> extends WrappingFunction<CrossFunction<IN1, IN2, OUT>>
	implements GenericCrosser<Reference<IN1>, Reference<IN2>, Reference<OUT>>
	{
		
		private static final long serialVersionUID = 1L;
		
		private final Reference<OUT> ref = new Reference<OUT>();
	
		protected ReferenceWrappingCrosser(
				CrossFunction<IN1, IN2, OUT> wrappedFunction) {
			super(wrappedFunction);
			// TODO Auto-generated constructor stub
		}


		@Override
		public void cross(Reference<IN1> record1, Reference<IN2> record2,
				Collector<Reference<OUT>> out) throws Exception {
			this.ref.ref = this.wrappedFunction.cross(record1.ref, record2.ref);


			out.collect(ref);
		}
	}

}
