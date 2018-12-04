package org.apache.flink.api.newConfiguration;

public interface ITupleVarDefinition
{
	String getType();
	
	int getMaxReservedBytes();
	
	<T> T getIdentityValue();
	
	boolean hasIdentityValue();
}
