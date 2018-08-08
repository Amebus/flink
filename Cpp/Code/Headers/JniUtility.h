#include "baseInclusion.h"
#include <jni.h>

#ifndef JniUtility_H
#define JniUtility_H


std::string GetStringFromJavaString(JNIEnv *pEnv, jstring pString);

void ReleaseJStringResources(JNIEnv *pEnv, jstring pJavaString, std::string pCppString); 

#endif //JniUtility_H