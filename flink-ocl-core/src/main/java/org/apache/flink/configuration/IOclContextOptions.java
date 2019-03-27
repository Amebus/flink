package org.apache.flink.configuration;

import java.io.Serializable;
import java.nio.ByteOrder;

public interface IOclContextOptions extends Serializable
{
	String getKernelsBuildFolder();
	
	boolean hasToRemoveTempFoldersOnClose();
	
	ByteOrder getNumbersByteOrdering();
}
