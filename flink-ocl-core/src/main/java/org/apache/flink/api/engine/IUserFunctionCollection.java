package org.apache.flink.api.engine;

import java.io.Serializable;

public interface IUserFunctionCollection<T extends IUserFunction> extends Iterable<T>, Serializable
{
	Iterable<T> getUserFunctions();
}
