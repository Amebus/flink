package org.apache.flink.streaming.configuration;

import java.io.Serializable;
import java.nio.ByteOrder;

public interface IOclContextOptions extends Serializable
{
	boolean hasToRemoveTempFoldersOnClose();
	
	ByteOrder getNumbersByteOrdering();
	
	String getKernelsBuildFolder();
	String getKernelSourcePath(String pKernelType);
}
