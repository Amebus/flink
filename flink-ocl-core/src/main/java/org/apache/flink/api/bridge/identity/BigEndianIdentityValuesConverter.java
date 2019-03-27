package org.apache.flink.api.bridge.identity;

import org.apache.flink.api.serialization.bigendian.BigEndianStreamWriter;
import org.apache.flink.api.tuple.IOclTuple;

public class BigEndianIdentityValuesConverter
	implements IIdentityConverterHelper
{
	@Override
	public IIdentityValuesStreamWriter getIdentityValuesStreamWriter()
	{
		return new BigEndianIdentityStreamWriter();
	}
	
	private static class BigEndianIdentityStreamWriter
		extends BigEndianStreamWriter
		implements IIdentityValuesStreamWriter
	{
		public int writeStream(IOclTuple pTuple, byte[] pStream, byte pArity)
		{
			return super.writeStream(pTuple, pStream, pArity, getTypes(pTuple));
		}
	}
}
