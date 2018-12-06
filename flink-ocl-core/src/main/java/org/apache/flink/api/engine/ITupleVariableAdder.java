package org.apache.flink.api.engine;

import org.apache.flink.api.oEngine.variable.VarDefinition;
import org.apache.flink.configuration.CTType;

import java.io.Serializable;
import java.util.Collection;

@FunctionalInterface
public interface ITupleVariableAdder extends Serializable
{
	void addTo(Collection<VarDefinition> pResult, CTType pType, int pIndex);
}
