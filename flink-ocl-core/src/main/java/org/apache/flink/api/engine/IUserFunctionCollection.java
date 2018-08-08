package org.apache.flink.api.engine;

public interface IUserFunctionCollection<T extends IUserFunction> extends Iterable<T>
{
	Iterable<T> getUserFunctions();
}
