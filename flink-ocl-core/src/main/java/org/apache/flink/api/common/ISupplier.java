package org.apache.flink.api.common;

@FunctionalInterface
public interface ISupplier<T, O>
{
	T get(O pOptions);
}
