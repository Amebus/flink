package org.apache.flink.streaming.api.ocl.bridge.identity;

import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;

public interface IIdentityValuesStreamWriter
{
	int writeStream(IOclTuple pTuple, byte[] pStream, byte pArity);
}
