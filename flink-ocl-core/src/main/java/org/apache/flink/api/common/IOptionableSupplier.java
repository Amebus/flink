package org.apache.flink.api.common;

@FunctionalInterface
public interface IOptionableSupplier<T, O>
{
	T get(O pOptions);
}
