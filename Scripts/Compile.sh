#!/bin/bash

#Headers directory
HDir=../Cpp/code/Headers
#Sources directory
SDir=../Cpp/Code/Sources
#OutputFile
OutFile=../Cpp/Out/libOCL.so

#$SDir/OclKernelInfoBuilder.cpp
#$SDir/OclKernelInfo.cpp

clear

#-Wl,-no-undefined

echo "compiling..."
g++ -std=c++11 -fPIC -shared $SDir/Ocl.cpp -o $OutFile -lOpenCL -I$JAVA_HOME/include -I$JAVA_HOME/include/linux
echo "done"

echo "execstack..."
sudo execstack -c $OutFile
echo "done"

OutFile=../Cpp/Out/libSerializationBridge.so

echo "compiling..."
g++ -std=c++11 -fPIC -shared $SDir/SerializationBridge.cpp -o $OutFile -lOpenCL -I$JAVA_HOME/include -I$JAVA_HOME/include/linux
echo "done"

echo "execstack..."
sudo execstack -c $OutFile
echo "done"


# OutFile=../Cpp/Out/libOclBridge.so

# echo "compiling..."
# g++ -std=c++11 -fPIC -shared $SDir/OclBridge.cpp -o $OutFile -lOpenCL -I$JAVA_HOME/include -I$JAVA_HOME/include/linux
# echo "done"

# echo "execstack..."
# sudo execstack -c $OutFile
# echo "done"

