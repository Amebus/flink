package org.apache.flink.configuration;

public interface ITupleDefinitionRepository
{
	int getTupleSupportedMaxArity();
	int getTupleSupportedMinArity();
	
	Iterable<String> getTupleSupportedTypes();
	Iterable<String> getTupleEngineTypes();
	
	Iterable<ITupleDefinition> getTupleDefinitions();
	ITupleDefinition getTupleDefinition(String pTupleName);
}
