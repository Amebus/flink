package org.apache.flink.utility;

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
