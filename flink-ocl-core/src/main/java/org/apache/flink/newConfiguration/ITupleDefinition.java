package org.apache.flink.api.newConfiguration;

public interface ITupleDefinition extends Iterable<ITupleVarDefinition>
{
	
	String getName();
	
	Byte getArity();
	
	ITupleVarDefinition getTVarDefinition(int pIndex);
	
	default boolean equals(ITupleDefinition pObj)
	{
		return pObj != null &&
			   getArity().equals(pObj.getArity()) &&
			   getName().equals(pObj.getName());
	}
}
