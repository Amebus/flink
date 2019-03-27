package org.apache.flink.api.serialization.littleendian;

import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple2Ocl;

public class Tuple2Iterator extends LittleEndianIterator
{
	Tuple2Iterator(StreamReader pStreamReader)
	{
		super(pStreamReader);
	}
	
	@Override
	public IOclTuple next()
	{
		Tuple2Ocl vTuple = new Tuple2Ocl();
		Object[] vValues = readValuesFromStream();
		
		vTuple.setField(vValues[0], 0);
		vTuple.setField(vValues[1], 1);
		
		return vTuple;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <R extends IOclTuple> R nextTuple()
	{
		return (R) next();
	}
}
