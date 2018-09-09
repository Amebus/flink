package org.apache.flink.streaming.common.utility;

public class StringHelper
{
	public static boolean isNullOrEmpty(String str)
	{
		return str == null || str.equals("");
	}
	
	public static boolean isNullOrWhiteSpace(String str)
	{
		return str == null || str.trim().equals("");
	}
}
