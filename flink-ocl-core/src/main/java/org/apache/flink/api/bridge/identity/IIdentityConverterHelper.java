package org.apache.flink.api.bridge.identity;

import org.apache.flink.api.bridge.IdentityValues;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.apache.flink.api.tuple.Tuple3Ocl;
import org.apache.flink.configuration.ITupleDefinition;
import org.apache.flink.configuration.ITupleVarDefinition;

public interface IIdentityConverterHelper
	extends IdentityValueToIdentityArrayConverter
{
	IIdentityValuesStreamWriter getIdentityValuesStreamWriter();
	
	default byte[] toIdentityArray(IdentityValues pIdentityValues)
	{
		ITupleDefinition vTuple = pIdentityValues.getTuple();
		Object[] vValues = new Object[vTuple.getArity()];
		byte[] mResult = new byte[pIdentityValues.getTupleDimension()];
		int i = 0;
		for (ITupleVarDefinition vVarDef : vTuple)
		{
			vValues[i] = vVarDef.getIdentityValue();
			i++;
		}
		
		getIdentityValuesStreamWriter()
			.writeStream(getTupleFromValues(vValues), mResult, vTuple.getArity());
		
		return mResult;
	}
	
	default IOclTuple getTupleFromValues(Object[] pValues)
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
}
