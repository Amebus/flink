package org.apache.flink.streaming.configuration;

import java.io.Serializable;

public interface IOclContextOptions extends Serializable
{
	String getKernelsBuildFolder();
	
	boolean hasToRemoveTempFoldersOnClose();
}
