package eu.stratosphere.example.java.japiDev;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.util.Collector;

public class CoGroupProg {

	public static class Multiplyer extends CoGroupFunction<Integer, Integer, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public void combineFirst(Iterator<Integer> records,
				Collector<Integer> out) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void combineSecond(Iterator<Integer> records,
				Collector<Integer> out) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void coGroup(Iterator<Integer> first, Iterator<Integer> second,
				Collector<Integer> out) throws Exception {
			
			while(first.hasNext()) {
				Integer i = first.next();
				while(second.hasNext()) {
					Integer j = second.next();
					
					out.collect(new Integer(i.intValue()*j.intValue()));
				}
			}
			
		}
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		// this will return the LocalExecutionContext, if invoked locally, and the ClusterExecutionContext, if invoked on the cluster
		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
		
		Collection<Integer> inputSet1 = (new HashSet<Integer>());
		inputSet1.add(1);
		inputSet1.add(2);
		inputSet1.add(3);
		inputSet1.add(4);
		inputSet1.add(5);
		inputSet1.add(6);
		inputSet1.add(7);
		inputSet1.add(8);
		inputSet1.add(9);
		inputSet1.add(10);
		
		Collection<Integer> inputSet2 = (new HashSet<Integer>());
		inputSet2.add(1);
		inputSet2.add(2);
		inputSet2.add(3);
		inputSet2.add(4);
		inputSet2.add(5);
		inputSet2.add(6);
		inputSet2.add(7);
		inputSet2.add(8);
		inputSet2.add(9);
		inputSet2.add(10);
		
		DataSet<Integer> input1 = context.fromCollection(inputSet1);
		DataSet<Integer> input2 = context.fromCollection(inputSet2);
		
		DataSet<Integer> basics = input1.coGroup(input2)
			.where(
					new KeySelector<Integer, Integer>() {
						@Override
						public Integer getKey(Integer value) {
							return value;
						}
					}
			).equalTo(
					new KeySelector<Integer, Integer>() {
						@Override
						public Integer getKey(Integer value) {
							return value;
						}
					}
			).with(new Multiplyer());
			
		basics.print();
		
		context.execute();
		
	}

}
