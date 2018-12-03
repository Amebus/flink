package org.apache.flink.api.newConfiguration;

import org.apache.flink.streaming.helpers.Constants;
import org.junit.Test;

import java.util.Iterator;

public class JsonTupleDefinitionTest
{
	@Test
	public void A()
	{
		ItupleDefinitionRepository vRepository =
			new JsonTupleRepository.Builder(Constants.RESOURCES_DIR).build();
		Iterable<ITupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
	}
	
}
