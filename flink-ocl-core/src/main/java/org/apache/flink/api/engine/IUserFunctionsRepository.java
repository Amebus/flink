package org.apache.flink.api.engine;

import java.io.Serializable;
import java.util.Collection;

public interface IUserFunctionsRepository extends Serializable
{
	IUserFunction getUserFunctionByName(String pUserFunctionName);
	
	Collection<IUserFunction> getUserFunctions();
	
	Iterable<String> getFunctionEngineTypes();
}
