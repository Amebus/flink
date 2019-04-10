package org.apache.flink.streaming.api.ocl.common;

@FunctionalInterface
public interface IOptionableSupplier<T, O>
{
	T get(O pOptions);
}
