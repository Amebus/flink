package org.apache.flink.streaming.api.ocl.common.utility;

import java.util.Arrays;

public class IterableHelper
{
	public static Iterable<String> getStringIterableFromArgs(String... args)
	{
		return getIterableFromArgs(args);
	}
	
	@SafeVarargs
	public static <R>Iterable<R> getIterableFromArgs(R... args)
	{
		return Arrays.asList(args);
	}
}
