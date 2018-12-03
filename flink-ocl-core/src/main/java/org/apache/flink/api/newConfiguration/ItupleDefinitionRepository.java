package org.apache.flink.api.newConfiguration;

public interface ItupleDefinitionRepository
{
	int getTupleSupportedMaxArity();
	int getTupleSupportedMinArity();
	
	Iterable<String> getTupleSupportedTypes();
	Iterable<String> getTupleEngineTypes();
	
	Iterable<ITupleDefinition> getTupleDefinitions();
	ITupleDefinition getTupleDefinition(String pTupleName);
}
