package org.apache.flink.streaming.api.ocl.bridge.identity;

import org.apache.flink.streaming.api.ocl.bridge.IdentityValues;

@FunctionalInterface
public interface IdentityValueToIdentityArrayConverter
{
	byte[] toIdentityArray(IdentityValues pIdentityValues);
}
