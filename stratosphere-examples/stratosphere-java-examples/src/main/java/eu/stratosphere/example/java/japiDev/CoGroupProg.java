package eu.stratosphere.example.java.japiDev;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class CoGroupProg {

	public static class Multiplyer extends CoGroupFunction<Tuple1<Integer>, Tuple1<Integer>, Tuple1<Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void combineFirst(Iterator<Tuple1<Integer>> records,
				Collector<Tuple1<Integer>> out) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void combineSecond(Iterator<Tuple1<Integer>> records,
				Collector<Tuple1<Integer>> out) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void coGroup(Iterator<Tuple1<Integer>> first, Iterator<Tuple1<Integer>> second,
				Collector<Tuple1<Integer>> out) throws Exception {

			while(first.hasNext()) {
				Tuple1<Integer> i = first.next();
				while(second.hasNext()) {
					Tuple1<Integer> j = second.next();
					
					out.collect(new Tuple1<Integer>(i.T1().intValue()+j.T1().intValue()));
				}
			}
			
		}
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		// this will return the LocalExecutionContext, if invoked locally, and the ClusterExecutionContext, if invoked on the cluster
		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
		
		Collection<Tuple1<Integer>> inputSet1 = (new HashSet<Tuple1<Integer>>());
		inputSet1.add(new Tuple1<Integer>(1));
		inputSet1.add(new Tuple1<Integer>(2));
		inputSet1.add(new Tuple1<Integer>(3));
		inputSet1.add(new Tuple1<Integer>(4));
		inputSet1.add(new Tuple1<Integer>(5));
		inputSet1.add(new Tuple1<Integer>(6));
		inputSet1.add(new Tuple1<Integer>(7));
		inputSet1.add(new Tuple1<Integer>(8));
		inputSet1.add(new Tuple1<Integer>(9));
		inputSet1.add(new Tuple1<Integer>(10));
		
		Collection<Tuple1<Integer>> inputSet2 = (new HashSet<Tuple1<Integer>>());
		inputSet2.add(new Tuple1<Integer>(1));
		inputSet2.add(new Tuple1<Integer>(2));
		inputSet2.add(new Tuple1<Integer>(3));
		inputSet2.add(new Tuple1<Integer>(4));
		inputSet2.add(new Tuple1<Integer>(5));
		inputSet2.add(new Tuple1<Integer>(6));
		inputSet2.add(new Tuple1<Integer>(7));
		inputSet2.add(new Tuple1<Integer>(8));
		inputSet2.add(new Tuple1<Integer>(9));
		inputSet2.add(new Tuple1<Integer>(10));
		
		DataSet<Tuple1<Integer>> input1 = context.fromCollection(inputSet1);
		DataSet<Tuple1<Integer>> input2 = context.fromCollection(inputSet2);
		
		DataSet<Tuple1<Integer>> basics = input1.coGroup(input2)
			.where(
					new KeySelector<Tuple1<Integer>, Integer>() {
						@Override
						public Integer getKey(Tuple1<Integer> value) {
							return value.getField(0);
						}
					}
			).equalTo(
					new KeySelector<Tuple1<Integer>, Integer>() {
						@Override
						public Integer getKey(Tuple1<Integer> value) {
							return value.getField(0);
						}
					}
			).with(new Multiplyer());
			
		basics.print();
		
		context.execute();
		
	}

}
