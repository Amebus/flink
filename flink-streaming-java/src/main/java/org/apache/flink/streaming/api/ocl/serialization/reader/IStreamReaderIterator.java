package org.apache.flink.streaming.api.ocl.serialization.reader;

import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;

import java.util.Iterator;

public interface IStreamReaderIterator extends Iterator<IOclTuple>
{
	<R extends IOclTuple> R nextTuple();
}
