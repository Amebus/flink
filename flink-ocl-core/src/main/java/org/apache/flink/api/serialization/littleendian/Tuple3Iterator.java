package org.apache.flink.api.serialization.littleendian;

import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple3Ocl;

public class Tuple3Iterator extends LittleEndianIterator
{
	Tuple3Iterator(StreamReader pStreamReader)
	{
		super(pStreamReader);
	}
	
	@Override
	public IOclTuple next()
	{
		Tuple3Ocl vTuple = new Tuple3Ocl();
		Object[] vValues = readValuesFromStream();
		
		vTuple.setField(vValues[0], 0);
		vTuple.setField(vValues[1], 1);
		vTuple.setField(vValues[2], 1);
		
		return vTuple;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <R extends IOclTuple> R nextTuple()
	{
		return (R)next();
	}
}
