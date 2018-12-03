package org.apache.flink.api.newEngine.user.functions;

import org.apache.flink.api.newEngine.IUserFunction;

public abstract class GenericUserFunction implements IUserFunction
{
	@Override
	public boolean equals(Object obj)
	{
		if(obj instanceof IUserFunction)
		{
			IUserFunction vOther = (IUserFunction)obj;
			return getName().equals(vOther.getName());
		}
		return false;
	}
}
