package org.apache.flink.api.newConfiguration;

import org.apache.flink.newConfiguration.ITupleDefinition;
import org.apache.flink.newConfiguration.ITupleDefinitionRepository;
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
