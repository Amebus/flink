package org.apache.flink.api.common;

@FunctionalInterface
public interface ISupplier<T>
{
	T get();
}