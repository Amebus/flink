package org.apache.flink.streaming.common;

@FunctionalInterface
public interface ISupplier<T, O>
{
	T get(O pOptions);
}
