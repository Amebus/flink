package org.apache.flink.streaming.api.ocl.engine.builder;

@FunctionalInterface
public interface FIGenerateExtra<T>
{
	T generateExtra();
}
