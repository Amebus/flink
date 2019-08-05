package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.configuration.ITupleDefinition;

@FunctionalInterface
public interface ITupleBytesDimensionGetter
{
	int getTupleDimension(ITupleDefinition pTupleDefinition);
}
