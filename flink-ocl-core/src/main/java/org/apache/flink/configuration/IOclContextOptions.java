package org.apache.flink.configuration;

public interface IOclContextOptions
{
	String getKernelsBuildFolder();
	
	boolean hasToRemoveTempFoldersOnClose();
}
