package org.apache.flink.api.serialization.bigendian;

import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.reader.IStreamReaderIterator;

public class BigEndianStreamReader extends StreamReader
{
	public IStreamReaderIterator streamReaderIterator()
	{
		switch (getArity())
		{
			case 1:
				return new Tuple1Iterator(this);
			case 2:
				return new Tuple2Iterator(this);
			case 3:
				return new Tuple3Iterator(this);
			default:
				throw new IllegalArgumentException(DIMENSION_ERROR);
		}
	}
}
