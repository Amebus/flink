package org.apache.flink.api.engine;

import org.apache.flink.configuration.ITupleDefinition;

@FunctionalInterface
public interface ITupleBytesDimensionGetters
{
	int getTupleDimension(ITupleDefinition pTupleDefinition);
}
