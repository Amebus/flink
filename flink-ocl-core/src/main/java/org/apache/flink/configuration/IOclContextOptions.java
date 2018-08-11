package org.apache.flink.configuration;

import java.io.Serializable;

public interface IOclContextOptions extends Serializable
{
	String getKernelsBuildFolder();
	
	boolean hasToRemoveTempFoldersOnClose();
}
