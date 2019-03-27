package org.apache.flink.api.serialization.bigendian;

import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;

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
