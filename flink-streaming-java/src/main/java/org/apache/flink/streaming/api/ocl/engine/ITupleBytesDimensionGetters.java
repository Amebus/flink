package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.configuration.ITupleDefinition;

@FunctionalInterface
public interface ITupleBytesDimensionGetters
{
	int getTupleDimension(ITupleDefinition pTupleDefinition);
}
