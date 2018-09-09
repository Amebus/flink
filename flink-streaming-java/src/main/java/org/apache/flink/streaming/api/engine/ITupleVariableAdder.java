package org.apache.flink.streaming.api.engine;

import org.apache.flink.streaming.configuration.CTType;
import org.apache.flink.streaming.api.engine.tuple.variable.VarDefinition;

import java.io.Serializable;
import java.util.Collection;

@FunctionalInterface
public interface ITupleVariableAdder extends Serializable
{
	void addTo(Collection<VarDefinition> pResult, CTType pType, int pIndex);
}
