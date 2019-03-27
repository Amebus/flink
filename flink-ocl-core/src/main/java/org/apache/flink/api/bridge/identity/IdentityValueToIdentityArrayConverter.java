package org.apache.flink.api.bridge.identity;

import org.apache.flink.api.bridge.IdentityValues;

@FunctionalInterface
public interface IdentityValueToIdentityArrayConverter
{
	byte[] toIdentityArray(IdentityValues pIdentityValues);
}
