package org.apache.flink.streaming.api.ocl.common;

@FunctionalInterface
public interface ISupplier<T>
{
	T get();
}
