package org.apache.flink.api.engine;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.flink.streaming.api.engine.IUserFunctionCollection;

import java.util.Iterator;
import java.util.List;

public class JsonUserFunctionCollection implements IUserFunctionCollection<JsonUserFunction>
{
	
	@SerializedName("functions")
	@Expose
	private List<JsonUserFunction> mUserFunctions;
	
	@Override
	public Iterable<JsonUserFunction> getUserFunctions()
	{
		return mUserFunctions;
	}
	
	@Override
	public Iterator<JsonUserFunction> iterator()
	{
		return mUserFunctions.iterator();
	}
}
