package org.apache.flink.api.bridge.identity;

import org.apache.flink.api.tuple.IOclTuple;

public interface IIdentityValuesStreamWriter
{
	int writeStream(IOclTuple pTuple, byte[] pStream, byte pArity);
}
