package org.apache.flink.configuration;

import java.io.Serializable;

public interface ITupleDefinitionsRepository<T extends ITupleDefinition> extends Serializable
{
	Iterable<T> getTupleDefinitions();
	T getTupleDefinition(String pTupleName);
}
