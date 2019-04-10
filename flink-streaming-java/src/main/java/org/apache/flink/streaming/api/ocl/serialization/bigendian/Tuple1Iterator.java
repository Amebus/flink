package org.apache.flink.streaming.api.ocl.serialization.bigendian;

import org.apache.flink.streaming.api.ocl.serialization.StreamReader;
import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;
import org.apache.flink.streaming.api.ocl.tuple.Tuple1Ocl;

public class Tuple1Iterator extends BigEndianIterator
{
	Tuple1Iterator(StreamReader pStreamReader)
	{
		super(pStreamReader);
	}
	
	@Override
	public IOclTuple next()
	{
		Tuple1Ocl vTuple = new Tuple1Ocl();
		Object[] vValues = readValuesFromStream();
		
		vTuple.setField(vValues[0], 0);
		
		return vTuple;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <R extends IOclTuple> R nextTuple()
	{
		return (R) next();
	}
}
