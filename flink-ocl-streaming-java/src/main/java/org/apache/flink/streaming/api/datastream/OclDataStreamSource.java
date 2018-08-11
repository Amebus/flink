package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;

public class OclDataStreamSource<T extends IOclTuple> extends DataStreamSource<T>
{
	public OclDataStreamSource(StreamExecutionEnvironment environment, TypeInformation<T> outTypeInfo, StreamSource<T, ?> operator, boolean isParallel, String sourceName)
	{
		super(environment, outTypeInfo, operator, isParallel, sourceName);
	}
	
	public OclDataStreamSource(SingleOutputStreamOperator<T> operator)
	{
		super(operator);
	}
}
