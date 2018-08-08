package org.apache.flink.api.engine;

import org.apache.flink.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.configuration.CTType;

import java.util.Collection;

@FunctionalInterface
public interface ITupleVariableAdder
{
	void addTo(Collection<VarDefinition> pResult, CTType pType, int pIndex);
}
