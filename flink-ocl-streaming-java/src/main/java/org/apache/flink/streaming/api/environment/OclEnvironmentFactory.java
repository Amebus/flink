package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.engine.IUserFunctionsRepository;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionsRepository;

public class OclEnvironmentFactory
{
	
	public static OclStreamExecutionEnvironment getExecutionEnvironment(
		ISettingsRepository pSettingsRepository,
		ITupleDefinitionsRepository pTupleDefinitionsRepository,
		IUserFunctionsRepository pUserFunctionsRepository)
	{
		StreamExecutionEnvironment vStreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		OclContext vOclContext = new OclContext(pSettingsRepository,
												pTupleDefinitionsRepository,
												pUserFunctionsRepository);
		
		return new OclStreamExecutionEnvironment(vStreamExecutionEnvironment, vOclContext);
	}
}
