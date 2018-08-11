package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OclOneInputOperator;
import org.apache.flink.streaming.api.operators.OclStreamFilter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;

public class OclDataStream<T extends IOclTuple>
{

	private OclContext mOclContext;
	
	public OclDataStream(OclContext pOclContext)
	{
		mOclContext = pOclContext;
	}
	
	public <R extends IOclTuple> OclSingleOutputStreamOperator<R> map(String pUserFunction){
		return null;
	}
}
