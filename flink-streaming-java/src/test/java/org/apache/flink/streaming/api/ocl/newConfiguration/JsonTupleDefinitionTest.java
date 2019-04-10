package org.apache.flink.streaming.api.ocl.newConfiguration;

import org.apache.flink.streaming.api.ocl.configuration.JsonTupleRepository;
import org.apache.flink.streaming.configuration.ITupleDefinition;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;
import org.apache.flink.streaming.helpers.Constants;
import org.junit.Test;

public class JsonTupleDefinitionTest
{
	@Test
	public void A()
	{
		ITupleDefinitionRepository vRepository =
			new JsonTupleRepository.Builder(Constants.RESOURCES_DIR).build();
		Iterable<ITupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
	}
	
}
