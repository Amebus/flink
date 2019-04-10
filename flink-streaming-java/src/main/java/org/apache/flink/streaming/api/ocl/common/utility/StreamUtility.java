package org.apache.flink.streaming.api.ocl.common.utility;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtility
{
	public static <T> Stream<T> streamFrom(Iterable<T> pIterable)
	{
		return streamFrom(pIterable, false);
	}
	
	public static <T> Stream<T> parallelStreamFrom(Iterable<T> pIterable)
	{
		return streamFrom(pIterable, true);
	}
	
	private static <T> Stream<T> streamFrom(Iterable<T> pIterable, boolean pParallel)
	{
		return StreamSupport.stream(pIterable.spliterator(), pParallel);
	}
}
