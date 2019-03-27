package org.apache.flink.api.bridge.identity;

import org.apache.flink.api.serialization.littleendian.LittleEndianStreamWriter;
import org.apache.flink.api.tuple.IOclTuple;

public class LittleEndianIdentityValuesConverter
	implements IIdentityConverterHelper
{
	@Override
	public IIdentityValuesStreamWriter getIdentityValuesStreamWriter()
	{
		return new LittleEndianIdentityStreamWriter();
	}
	
	private static class LittleEndianIdentityStreamWriter
		extends LittleEndianStreamWriter
		implements IIdentityValuesStreamWriter
	{
		public int writeStream(IOclTuple pTuple, byte[] pStream, byte pArity)
		{
			return super.writeStream(pTuple, pStream, pArity, getTypes(pTuple));
		}
	}
}
