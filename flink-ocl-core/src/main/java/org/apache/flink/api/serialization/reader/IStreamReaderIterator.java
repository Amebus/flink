package org.apache.flink.api.serialization.reader;

import org.apache.flink.api.tuple.IOclTuple;

import java.util.Iterator;

public interface IStreamReaderIterator extends Iterator<IOclTuple>
{
	<R extends IOclTuple> R nextTuple();
}
