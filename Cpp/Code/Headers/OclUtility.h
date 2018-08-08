
#include "baseInclusion.h"
#include <CL/cl.hpp>

#ifndef OclUtility_H
#define OclUtility_H

void printStatus(const cl_int status, const int line);

void PrintClError(cl::Error* error);

std::string GetKernelNameFromKernelFileName(std::string pKernelName);

std::vector<std::string> GetKernelsSourceFiles(std::string pKernelsFolder);

std::string GetKernelSourceCode(std::string pFile);

cl::Program CompileKernelProgram(std::string pSourceCode);

void StoreKernelProgram(std::string pKernelName, cl::Program pKernelProgram);

void CompileAndStoreOclKernel(std::string pKernelsFolder, std::string pKernelName);

void CompileAndStoreOclKernels(std::string pKernelsFolder, std::vector<std::string> pKernelsFiles);

#endif //OclUtility_H