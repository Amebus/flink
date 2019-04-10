package org.apache.flink.streaming.configuration;

public interface ITupleVarDefinition
{
	String getType();
	
	int getMaxReservedBytes();
	
	<T> T getIdentityValue();
	
	boolean hasIdentityValue();
	
	int getIndex();
}
