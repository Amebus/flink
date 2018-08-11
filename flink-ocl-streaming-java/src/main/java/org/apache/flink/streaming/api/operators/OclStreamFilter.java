package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.List;

public class OclStreamFilter<IN extends IOclTuple>
	extends OclAbstractUdfStreamOperator<IN>
	implements OclOneInputOperator<IN, IN>
{
	public OclStreamFilter(String pUserFunction)
	{
		super(pUserFunction);
	}
	
	
	/**
	 * Is called by the {@link StreamGraph#addOperator(Integer, String, StreamOperator, TypeInformation, TypeInformation, String)}
	 * method when the {@link StreamGraph} is generated. The
	 * method is called with the output {@link TypeInformation} which is also used for the
	 * {@link StreamTask} output serializer.
	 *
	 * @param outTypeInfo     Output type information of the {@link StreamTask}
	 * @param executionConfig Execution configuration
	 */
	@Override
	public void setOutputType(TypeInformation<IN> outTypeInfo, ExecutionConfig executionConfig)
	{
	
	}
	
	/**
	 * Processes one element that arrived at this operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @param elements
	 */
	@Override
	public void processElements(List<IN> elements) throws Exception
	{
	
	}
}
