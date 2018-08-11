package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.tuple.IOclTuple;

public class OclSingleOutputStreamOperator<T extends IOclTuple> extends OclDataStream<T>
{
	public OclSingleOutputStreamOperator(OclContext pOclContext)
	{
		super(pOclContext);
	}
}
