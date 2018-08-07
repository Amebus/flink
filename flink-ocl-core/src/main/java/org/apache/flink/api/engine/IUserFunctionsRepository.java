package org.apache.flink.api.engine;

import java.util.Collection;

public interface IUserFunctionsRepository
{
	IUserFunction getUserFunctionByName(String pUserFunctionName);
	
	Collection<IUserFunction> getUserFunctions();
}
