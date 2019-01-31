package org.apache.flink.api.bridge;

import org.apache.flink.api.serialization.StreamWriter;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.apache.flink.api.tuple.Tuple3Ocl;
import org.apache.flink.configuration.ITupleDefinition;
import org.apache.flink.configuration.ITupleVarDefinition;

public class IdentityValues
{
	private ITupleDefinition mTuple;
	private int mTupleDimension;
	
	public IdentityValues(ITupleDefinition pTuple, int pTupleDimension)
	{
		mTuple = pTuple;
		
		mTupleDimension = pTupleDimension;
	}
	
	byte[] toIdentityArray()
	{
		Object[] vValues = new Object[mTuple.getArity()];
		byte[] mResult = new byte[mTupleDimension];
		int i = 0;
		for (ITupleVarDefinition vVarDef : mTuple)
		{
			vValues[i] = vVarDef.getIdentityValue();
			i++;
		}
		
		
		IdentityStreamWriter vStreamWriter = new IdentityStreamWriter();
		
		vStreamWriter.writeStream(vValues, mResult, mTuple.getArity());
		
		return mResult;
	}
	
	
	private static class IdentityStreamWriter extends StreamWriter
	{
		private IOclTuple getTupleFromValues(Object[] pValues)
		{
			IOclTuple vResultTuple;
			switch (pValues.length)
			{
				case 1:
					vResultTuple = new Tuple1Ocl<>(pValues[0]);
					break;
				case 2:
					vResultTuple = new Tuple2Ocl<>(pValues[0], pValues[1]);
					break;
				case 3:
					vResultTuple = new Tuple3Ocl<>(pValues[0], pValues[1], pValues[2]);
					break;
				default:
					throw new IllegalArgumentException("Arity not supported");
			}
			return vResultTuple;
		}
		
		protected int writeStream(Object[] pValues, byte[] pStream, byte pArity)
		{
			IOclTuple vTuple = getTupleFromValues(pValues);
			return super.writeStream(vTuple, pStream, pArity, getTypes(vTuple));
		}
	}
}
