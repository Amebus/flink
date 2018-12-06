package org.apache.flink.api.engine;

import org.apache.flink.newConfiguration.ITupleDefinition;

@FunctionalInterface
public interface ITupleBytesDimensionGetters
{
	int getTupleDimension(ITupleDefinition pTupleDefinition);
}
