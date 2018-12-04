package org.apache.flink.newConfiguration;

public interface ITupleVarDefinition
{
	String getType();
	
	int getMaxReservedBytes();
	
	<T> T getIdentityValue();
	
	boolean hasIdentityValue();
	
	int getIndex();
}
