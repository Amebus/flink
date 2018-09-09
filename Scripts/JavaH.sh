#!/bin/bash

javah -d ../Cpp/Code/Headers -jni -classpath ../flink-streaming-java/target/classes org.apache.flink.streaming.api.bridge.AbstractOclBridge

