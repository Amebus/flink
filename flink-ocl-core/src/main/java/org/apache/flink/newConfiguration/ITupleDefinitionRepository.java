package org.apache.flink.api.newConfiguration;

public interface ITupleDefinitionRepository
{
	int getTupleSupportedMaxArity();
	int getTupleSupportedMinArity();
	
	Iterable<String> getTupleSupportedTypes();
	Iterable<String> getTupleEngineTypes();
	
	Iterable<ITupleDefinition> getTupleDefinitions();
	ITupleDefinition getTupleDefinition(String pTupleName);
}
