package org.apache.flink.api.engine;

import org.apache.flink.utility.StringHelper;

public abstract class UserFunction implements IUserFunction
{
	protected void checkType()
	{
		if(isOfUnknownType())
			throw new IllegalArgumentException("The function \"" + getName() + "\" is of type \"" + getType() + "\" which is unknown.");
	}
	
	protected void checkOutputTuple()
	{
		if(doesNotHaveOutputTupleSpecified() && hasOutputTuple())
		{
			throw new IllegalArgumentException("The function \"" + getName() + "\" is a " + getType() + " and therefore should return a tuple.\n" +
											   "Change the function type or specify an output tuple.");
		}
	}
	
	protected boolean doesNotHaveOutputTupleSpecified()
	{
		return StringHelper.isNullOrWhiteSpace(getOutputTupleName());
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if(obj instanceof UserFunction)
		{
			IUserFunction vOther = (UserFunction)obj;
			return equals(vOther);
		}
		return false;
	}
}
