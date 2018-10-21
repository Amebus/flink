#define __CL_ENABLE_EXCEPTIONS
#include "../Headers/org_apache_flink_api_bridge_AbstractOclBridge.h"
#include "../Headers/OclUtility.h"
#include "../Headers/JniUtility.h"
#include <fstream>
#include <iostream>
#include <dirent.h>
#include <iomanip>
#include <unordered_map>

void printStatus(const cl_int status, const int line)
{
    if (status != CL_SUCCESS)
    {
        std::stringstream msg;
        msg << "Line " << std::setw(4) << line << ": ";
        switch (status)
        {
            case CL_DEVICE_NOT_FOUND:
                msg << "Device not found.";
                break;
            case CL_DEVICE_NOT_AVAILABLE:
                msg << "Device not available";
                break;
            case CL_COMPILER_NOT_AVAILABLE:
                msg << "Compiler not available";
                break;
            case CL_MEM_OBJECT_ALLOCATION_FAILURE:
                msg << "Memory object allocation failure";
                break;
            case CL_OUT_OF_RESOURCES:
                msg << "Out of resources";
                break;
            case CL_OUT_OF_HOST_MEMORY:
                msg << "Out of host memory";
                break;
            case CL_PROFILING_INFO_NOT_AVAILABLE:
                msg << "Profiling information not available";
                break;
            case CL_MEM_COPY_OVERLAP:
                msg << "Memory copy overlap";
                break;
            case CL_IMAGE_FORMAT_MISMATCH:
                msg << "Image format mismatch";
                break;
            case CL_IMAGE_FORMAT_NOT_SUPPORTED:
                msg << "Image format not supported";
                break;
            case CL_BUILD_PROGRAM_FAILURE:
                msg << "Program build failure";
                break;
            case CL_MAP_FAILURE:
                msg << "Map failure";
                break;
            case CL_INVALID_VALUE:
                msg << "Invalid value";
                break;
            case CL_INVALID_DEVICE_TYPE:
                msg << "Invalid device type";
                break;
            case CL_INVALID_PLATFORM:
                msg << "Invalid platform";
                break;
            case CL_INVALID_DEVICE:
                msg << "Invalid device";
                break;
            case CL_INVALID_CONTEXT:
                msg << "Invalid context";
                break;
            case CL_INVALID_QUEUE_PROPERTIES:
                msg << "Invalid queue properties";
                break;
            case CL_INVALID_COMMAND_QUEUE:
                msg << "Invalid command queue";
                break;
            case CL_INVALID_HOST_PTR:
                msg << "Invalid host pointer";
                break;
            case CL_INVALID_MEM_OBJECT:
                msg << "Invalid memory object";
                break;
            case CL_INVALID_IMAGE_FORMAT_DESCRIPTOR:
                msg << "Invalid image format descriptor";
                break;
            case CL_INVALID_IMAGE_SIZE:
                msg << "Invalid image size";
                break;
            case CL_INVALID_SAMPLER:
                msg << "Invalid sampler";
                break;
            case CL_INVALID_BINARY:
                msg << "Invalid binary";
                break;
            case CL_INVALID_BUILD_OPTIONS:
                msg << "Invalid build options";
                break;
            case CL_INVALID_PROGRAM:
                msg << "Invalid program";
                break;
            case CL_INVALID_PROGRAM_EXECUTABLE:
                msg << "Invalid program executable";
                break;
            case CL_INVALID_KERNEL_NAME:
                msg << "Invalid kernel name";
                break;
            case CL_INVALID_KERNEL_DEFINITION:
                msg << "Invalid kernel definition";
                break;
            case CL_INVALID_KERNEL:
                msg << "Invalid kernel";
                break;
            case CL_INVALID_ARG_INDEX:
                msg << "Invalid argument index";
                break;
            case CL_INVALID_ARG_VALUE:
                msg << "Invalid argument value";
                break;
            case CL_INVALID_ARG_SIZE:
                msg << "Invalid argument size";
                break;
            case CL_INVALID_KERNEL_ARGS:
                msg << "Invalid kernel arguments";
                break;
            case CL_INVALID_WORK_DIMENSION:
                msg << "Invalid work dimension";
                break;
            case CL_INVALID_WORK_GROUP_SIZE:
                msg << "Invalid work group size";
                break;
            case CL_INVALID_WORK_ITEM_SIZE:
                msg << "Invalid work item size";
                break;
            case CL_INVALID_GLOBAL_OFFSET:
                msg << "Invalid global offset";
                break;
            case CL_INVALID_EVENT_WAIT_LIST:
                msg << "Invalid event wait list";
                break;
            case CL_INVALID_EVENT:
                msg << "Invalid event";
                break;
            case CL_INVALID_OPERATION:
                msg << "Invalid operation";
                break;
            case CL_INVALID_GL_OBJECT:
                msg << "Invalid OpenGL object";
                break;
            case CL_INVALID_BUFFER_SIZE:
                msg << "Invalid buffer size";
                break;
            case CL_INVALID_MIP_LEVEL:
                msg << "Invalid mip-map level";
                break;
            default:
                msg << "Unknown";
                break;
        }
        msg << "\n";
        std::cout << msg.str().c_str();
    }
}

void PrintClError(cl::Error* error)
{
	std::cout << "Error" << std::endl;
	std::cout << error->what() << "(" << error->err() << ")" << std::endl;
}

std::unordered_map<std::string, cl::Program> gProgrmasList;
//std::unordered_map<std::string, cl_program> gProgramsList;
std::vector<cl::Platform> gPlatforms;
std::vector<cl::Device> gDevices;

cl::Platform gPlatform;
cl::Device gDefaultDevice;
cl::Context gContext;
cl::CommandQueue gCommandQueue;
cl_int gStatus;

#define printStatus() printStatus(gStatus, __LINE__)

#pragma region dispose definition

void DisposePrograms();
void DisposeDevices();
void DisposeCommandQueue();
void DisposeContext();

#pragma endregion

#pragma region Classes

class OclKernelAdditionalArgument
{
    private:
        unsigned char* mValue;
        int mLength;
        size_t mSize;
    public:
        OclKernelAdditionalArgument(unsigned char* pValue, int pLength)
        {
            mValue = pValue;
            mLength = pLength;
            mSize = sizeof(unsigned char) * mLength;
        }
        virtual ~OclKernelAdditionalArgument()
        {

        }
        unsigned char* GetValue()
        {
            return mValue;
        }
        int GetLength()
        {
            return mLength;
        }
        size_t GetSize()
        {
            return mSize;
        }
};

class OclKernelExecutionInfo
{
    protected:

        JNIEnv *mEnv;
        jobject mObj;
        std::string mKernelName;


        unsigned char *mStream, *mResult;
        int* mIndexes;

        std::vector<OclKernelAdditionalArgument*> mAdditionalArguments;

        int mStreamLength, mIndexesLegth, mResultLength;
        size_t mStreamSize, mIndexesSize, mResultSize;

        virtual void SetUpResult()
        {

        };
    public:
        OclKernelExecutionInfo (JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes)
        {
            mEnv = pEnv;
            mObj = pObj;
            mKernelName = GetStringFromJavaString(mEnv, pKernelName);

            this->SetUpStream(pStream)->SetUpIndexes(pIndexes)->SetUpResult();
        }

        virtual ~OclKernelExecutionInfo ()
        {
        }

        #pragma region getters
        std::string GetKernelName()
        {
            return mKernelName;
        }
        const char* GetCharKernelName()
        {
            return mKernelName.c_str();
        }

        unsigned char* GetStream()
        {
            return mStream;
        }
        size_t GetStreamSize()
        {
            return mStreamSize;
        }
        int GetStreamLength();


        int* GetIndexes()
        {
            return mIndexes;
        }
        size_t GetIndexesSize()
        {
            return mIndexesSize;
        }
        int GetIndexesLength()
        {
            return mIndexesLegth;
        }

        virtual unsigned char* GetResult()
        {
            return mResult;
        }
        size_t GetResultSize()
        {
            return mResultSize;
        }
        int GetResultLength()
        {
            return mResultLength;
        }

        int GetAdditionalArgumentsCount()
        {
            return mAdditionalArguments.size();
        }

        OclKernelAdditionalArgument* GetAdditionalArgument(int pIndex)
        {
            return mAdditionalArguments.at(pIndex);
        }

        #pragma endregion

        #pragma region setters
        virtual OclKernelExecutionInfo* SetUpStream(jbyteArray pStream)
        {
            mStreamLength = mEnv->GetArrayLength(pStream);
            mStream = (unsigned char *)mEnv->GetByteArrayElements(pStream, 0);
            mStreamSize = sizeof(unsigned char) * mStreamLength;
            return this;
        }

        virtual OclKernelExecutionInfo* SetUpIndexes(jintArray pIndexes)
        {
            mIndexesLegth = mEnv->GetArrayLength(pIndexes);
            mIndexes = mEnv->GetIntArrayElements(pIndexes, 0);
            mIndexesSize = sizeof(int) * mIndexesLegth;
            return this;
        }

        virtual OclKernelExecutionInfo* AddAdditionalArgument(jbyteArray pAdditionalArgument)
        {
            int vLength = mEnv->GetArrayLength(pAdditionalArgument);
            unsigned char * vValue = (unsigned char *)mEnv->GetByteArrayElements(pAdditionalArgument, 0);
            return this->AddAdditionalArgument(vValue, vLength);
        }
        virtual OclKernelExecutionInfo* AddAdditionalArgument(unsigned char* pValue, int pLength)
        {
            mAdditionalArguments.push_back(new OclKernelAdditionalArgument(pValue, pLength));
            return this;
        }

        #pragma endregion

        jbooleanArray ToJBooleanArray();
        jbyteArray ToJbyteArray();
};

class OclFilterExecutionInfo : public OclKernelExecutionInfo
{
    protected:
        void SetUpResult()
        {
            mResultLength = mIndexesLegth;
            mResultSize = sizeof(unsigned char) * mResultLength;
            mResult = new unsigned char[mResultLength];
        }
    public:
        OclFilterExecutionInfo (JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes)
            : OclKernelExecutionInfo (pEnv, pObj, pKernelName, pStream, pIndexes)
        {
            this->SetUpResult();
        }
};

class OclKernelExecutionInfoForOutputTuple : public OclKernelExecutionInfo
{
    protected:
        int mOutputTupleDimension;
        int mInfoLength;
        unsigned char* mInfo;
    public:
        OclKernelExecutionInfoForOutputTuple (JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
                                jint pOutputTupleDimension, jbyteArray pOutputTupleInfo)
            : OclKernelExecutionInfo (pEnv, pObj, pKernelName, pStream, pIndexes)
        {
            mOutputTupleDimension = pOutputTupleDimension;
            
            mInfoLength = mEnv->GetArrayLength(pOutputTupleInfo);
            mInfo = (unsigned char *)mEnv->GetByteArrayElements(pOutputTupleInfo, 0);
        }
        int GetOutputTupleDimension()
        {
            return mOutputTupleDimension;
        }
        unsigned char* GetResult()
        {
            for(int i = 0; i < mInfoLength; i++)
            {
                mResult[i] = mInfo[i];
            }
            return mResult;
        }
};

class OclMapExecutionInfo : public OclKernelExecutionInfoForOutputTuple
{
    protected:
        void SetUpResult()
        {
            mResultLength = mInfoLength + mIndexesLegth * mOutputTupleDimension;
            mResultSize = sizeof(unsigned char) * mResultLength;
            mResult = new unsigned char[mResultLength];
        }
    public:
        OclMapExecutionInfo (JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
                                jint pOutputTupleDimension, jbyteArray pOutputTupleInfo)
            : OclKernelExecutionInfoForOutputTuple (pEnv, pObj, pKernelName, pStream, pIndexes, 
                pOutputTupleDimension, pOutputTupleInfo)
        {
            this->SetUpResult();
        }
};

class OclReduceExecutionInfo : public OclKernelExecutionInfoForOutputTuple
{
    protected:
        void SetUpResult()
        {
            mResultLength = mInfoLength + mOutputTupleDimension;
            mResultSize = sizeof(unsigned char) * mResultLength;
            mResult = new unsigned char[mResultLength];
        }
    public:
        OclReduceExecutionInfo (JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
                                jint pOutputTupleDimension, jbyteArray pOutputTupleInfo)
            : OclKernelExecutionInfoForOutputTuple (pEnv, pObj, pKernelName, pStream, pIndexes, pOutputTupleDimension, pOutputTupleInfo)
        {
            this->SetUpResult();
        }
};

#pragma endregion

#pragma region Java native implementation

void RunKernel(OclKernelExecutionInfo* pKernelInfo)
{
    size_t vStreamSize, vIndexesSize, vResultSize, vArgSize;
    vStreamSize = pKernelInfo->GetStreamSize();
    vIndexesSize = pKernelInfo->GetIndexesSize();
    vResultSize = pKernelInfo->GetResultSize();
    std::vector<cl::Buffer> vBuffers;
    try
    {
        int vArgIndex = 0;
        cl::Kernel vKernel = cl::Kernel(gProgrmasList[pKernelInfo->GetKernelName()], pKernelInfo->GetCharKernelName());

        cl::Buffer vStreamBuffer(gContext, CL_MEM_READ_ONLY, vStreamSize);
        cl::Buffer vIndexesBuffer(gContext, CL_MEM_READ_ONLY, vIndexesSize);
        cl::Buffer vResultBuffer(gContext, CL_MEM_WRITE_ONLY, vResultSize);

        gCommandQueue.enqueueWriteBuffer(vStreamBuffer, CL_TRUE, 0, vStreamSize, pKernelInfo->GetStream());
        gCommandQueue.enqueueWriteBuffer(vIndexesBuffer, CL_TRUE, 0, vIndexesSize, pKernelInfo->GetIndexes());

        vKernel.setArg(vArgIndex++, vStreamBuffer);
        vKernel.setArg(vArgIndex++, vIndexesBuffer);
        vKernel.setArg(vArgIndex++, vResultBuffer);

        int vAdditionalArgumentsCount = pKernelInfo->GetAdditionalArgumentsCount();
        OclKernelAdditionalArgument* vArg;
        if( vAdditionalArgumentsCount > 0)
        {
            for(int vI = 0; vI < vAdditionalArgumentsCount; vI++, vArgIndex++)
            {
                vArg = pKernelInfo->GetAdditionalArgument(vI);
                vArgSize = vArg->GetSize();
                cl::Buffer vBuffer(gContext, CL_MEM_READ_ONLY, vArgSize);
                gCommandQueue.enqueueWriteBuffer(vBuffer, CL_TRUE, 0, vArgSize, vArg->GetValue());
                vKernel.setArg(vArgIndex, vBuffer);
            }
        }

        cl::NDRange global(pKernelInfo->GetIndexesLength());

        gCommandQueue.enqueueNDRangeKernel(vKernel, cl::NullRange, global, cl::NullRange);

        gCommandQueue.enqueueReadBuffer(vResultBuffer, CL_TRUE, 0, vResultSize, pKernelInfo->GetResult());
    }
    catch(cl::Error *vError)
    {
        PrintClError(vError);
        exit(1);
    }
}

JNIEXPORT void Java_org_apache_flink_api_bridge_AbstractOclBridge_ListDevices(JNIEnv *pEnv, jobject pObj)
{
    cl_uint numPlatforms = 0;
    gStatus = CL_SUCCESS;

    // Get (in numPlatforms) the number of OpenCL platforms available
    // No platform ID will be return, since platforms is NULL
    gStatus = clGetPlatformIDs(0, NULL, &numPlatforms);
	printStatus();

	std::cout << "numPlatforms:" << numPlatforms << '\n' << '\n';

	std::vector<cl_platform_id> platforms(numPlatforms);

    // Now, obtains a list of numPlatforms OpenCL platforms available
    // The list of platforms available will be returned in platforms
    gStatus = clGetPlatformIDs(numPlatforms, &platforms[0], NULL);
	printStatus();

	size_t stringLength = 0;
	for (cl_uint i = 0; i < numPlatforms; i++)
    {
		cl_platform_id platformId = platforms[i];
		cl_uint numDevices = 0;

		// In order to read the platform's name, we first read the platform's name string length (param_value is NULL).
		// The value returned in stringLength
		gStatus = clGetPlatformInfo(platformId, CL_PLATFORM_NAME, 0, NULL, &stringLength);
		printStatus();

		// Now, that we know the platform's name string length, we can allocate enough space before read vEntry
	    std::vector<char> platformName(stringLength);

		// Read the platform's name string
	    // The read value returned in platformName
	    gStatus = clGetPlatformInfo(platformId, CL_PLATFORM_NAME, stringLength, &platformName[0], NULL);
		printStatus();

		std::cout << "\tplatformName: ";

		for (size_t i = 0; i < stringLength - 1; i++) {
			std::cout << platformName[i];
		}

		std::cout << '\n';

		// Obtains the number of deviceType devices available on platform
		// When the function failed we expect numDevices to be zero.
		// We ignore the function return value since a non-zero error code
		// could happen if this platform doesn't support the specified device type.
		gStatus = clGetDeviceIDs(platformId, CL_DEVICE_TYPE_ALL , 0, NULL, &numDevices);
		printStatus();

		std::vector<cl_device_id> devices(numDevices);

		gStatus = clGetDeviceIDs(platformId, CL_DEVICE_TYPE_ALL , numDevices, &devices[0], NULL);
		printStatus();

		gStatus = clGetPlatformInfo(platformId, CL_PLATFORM_VERSION, 0, NULL, &stringLength);
		printStatus();

		// Now, that we know the platform's version string length, we can allocate enough space before read vEntry
	    std::vector<char> platformVersion(stringLength);

		// Read the platform's version string
	    // The read value returned in platformVersion
	    gStatus = clGetPlatformInfo(platformId, CL_PLATFORM_VERSION, stringLength, &platformVersion[0], NULL);
		printStatus();

		std::cout << "\tplatformVersion: ";

		for (size_t i = 0; i < stringLength - 1; i++) {
			std::cout << platformVersion[i];
		}

		std::cout << '\n';

		std::cout << "\t devices: " << numDevices << '\n';

		for (size_t j = 0; j < numDevices; j++) {
			cl_device_id device = devices[j];
			// Read the device's version string length (param_value is NULL).
		    gStatus = clGetDeviceInfo(device, CL_DEVICE_VERSION, 0, NULL, &stringLength);
			printStatus();

			std::vector<char> version(stringLength);

			// Read the device's version string
		    // The read value returned in deviceVersion
		    gStatus = clGetDeviceInfo(device, CL_DEVICE_VERSION, stringLength, &version[0], NULL);
			printStatus();

			std::cout << "\t\tdeviceVersion: ";

			for (size_t i = 0; i < stringLength - 1; i++) {
				std::cout << version[i];
			}

			std::cout << '\n';

			// Read the device's OpenCL C version string length (param_value is NULL).
		    gStatus = clGetDeviceInfo(device, CL_DEVICE_OPENCL_C_VERSION, 0, NULL, &stringLength);
			printStatus();

			// Now, that we know the device's OpenCL C version string length, we can allocate enough space before read vEntry
			std::vector<char> compilerVersion(stringLength);

			// Read the device's OpenCL C version string
			// The read value returned in compilerVersion
			gStatus = clGetDeviceInfo(device, CL_DEVICE_OPENCL_C_VERSION, stringLength, &compilerVersion[0], NULL);
			printStatus();

			std::cout << "\t\tdeviceCompilerVersion: ";

			for (size_t i = 0; i < stringLength - 1; i++) {
				std::cout << compilerVersion[i];
			}

			std::cout << '\n';

		}


		std::cout << '\n';
	}
}

JNIEXPORT void Java_org_apache_flink_api_bridge_AbstractOclBridge_Initialize(JNIEnv *pEnv, jobject pObj, jstring pKernelsFolder)
{
    //TODO improve to accepet external parameters
    std::string vKernelsFolder = GetStringFromJavaString(pEnv, pKernelsFolder);

    std::vector<std::string> vKernelsfiles = GetKernelsSourceFiles(vKernelsFolder);
    try
    {
        cl::Platform::get(&gPlatforms);
        gPlatforms[0].getDevices(CL_DEVICE_TYPE_ALL, &gDevices);
        gDefaultDevice = gDevices[0];
        gContext = cl::Context(gDefaultDevice);
	    gCommandQueue = cl::CommandQueue(gContext, gDefaultDevice);
        CompileAndStoreOclKernels(vKernelsFolder, vKernelsfiles);
    }
    catch(cl::Error* vError)
    {
        PrintClError(vError);
        exit(1);
    }
}

JNIEXPORT void Java_org_apache_flink_api_bridge_AbstractOclBridge_Dispose(JNIEnv *pEnv, jobject pObj)
{
    DisposePrograms();
    DisposeDevices();
    
    DisposeCommandQueue();
    DisposeContext();
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_flink_api_bridge_AbstractOclBridge_OclMap(
    JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
    jint pOutputTupleDimension, jbyteArray pOutputTupleInfo)
{
    OclKernelExecutionInfo *vKernelInfo = new OclMapExecutionInfo(pEnv, pObj, pKernelName, pStream, pIndexes, pOutputTupleDimension, pOutputTupleInfo);
    
    RunKernel(vKernelInfo);

    jbyteArray vRet = pEnv->NewByteArray(vKernelInfo->GetResultLength());
	pEnv->SetByteArrayRegion(vRet, 0, vKernelInfo->GetResultLength(), (signed char *)vKernelInfo->GetResult());

    return vRet;
}

JNIEXPORT jbooleanArray JNICALL 
Java_org_apache_flink_api_bridge_AbstractOclBridge_OclFilter(
    JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes)
{
    std::string vKernelName = GetStringFromJavaString(pEnv, pKernelName);

    unsigned char* vStream;
    int* vIndexes;

    int vStreamLength, vIndexesLegth, vResultLength;
    size_t vStreamSize, vIndexesSize, vResultSize;
    OclFilterExecutionInfo *vKernelInfo = new OclFilterExecutionInfo(pEnv, pObj, pKernelName, pStream, pIndexes);

    RunKernel(vKernelInfo);

    jbooleanArray vRet = pEnv->NewBooleanArray(vKernelInfo->GetResultLength());
	pEnv->SetBooleanArrayRegion(vRet, 0, vKernelInfo->GetResultLength(), vKernelInfo->GetResult());

    return vRet;
}

JNIEXPORT jbyteArray JNICALL 
Java_org_apache_flink_api_bridge_AbstractOclBridge_OclReduce(
    JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
    jint pOutputTupleDimension, jbyteArray pOutputTupleInfo)
{

    OclKernelExecutionInfo *vKernelInfo = new OclReduceExecutionInfo(pEnv, pObj, pKernelName, pStream, pIndexes, pOutputTupleDimension, pOutputTupleInfo);

    RunKernel(vKernelInfo);

    jbyteArray vRet = pEnv->NewByteArray(vKernelInfo->GetResultLength());
	pEnv->SetByteArrayRegion(vRet, 0, vKernelInfo->GetResultLength(), (signed char *)vKernelInfo->GetResult());

    return vRet;
}

#pragma endregion

#pragma region Dispose implementation

void DisposePrograms()
{
    // for (auto vEntry : gProgrmasList) 
    // {
    //     clReleaseProgram(vEntry.second);
    //     vEntry.second.
    // }
    // gProgrmasList.clear();
}

void DisposeDevices()
{
    // for(auto vDevice : gDevices)
    // {
    //     clReleaseDevice(vDevice);
    // }
    // gDevices.clear();
    // clReleaseDevice(gDefaultDevice);
}

void DisposeCommandQueue()
{
    // clReleaseCommandQueue(gCommandQueue);
}

void DisposeContext()
{
    // clReleaseContext(gContext);
}

#pragma endregion

#pragma region Kernels build and storage

void CompileAndStoreOclKernels(std::string pKernelsFolder, std::vector<std::string> pKernelsFiles)
{
    for(auto vkernelFile: pKernelsFiles) 
    {
        CompileAndStoreOclKernel(pKernelsFolder, vkernelFile);
    }
}

void CompileAndStoreOclKernel(std::string pKernelsFolder, std::string pKernelName)
{
    std::string vFullName(pKernelsFolder + "/" + pKernelName);

    std::string vKernelName = GetKernelNameFromKernelFileName(pKernelName);

    std::string vSourceCode = GetKernelSourceCode(vFullName);

    cl::Program vProgram = CompileKernelProgram(vSourceCode);

    StoreKernelProgram(vKernelName, vProgram);
}

std::string GetKernelNameFromKernelFileName(std::string pKernelName)
{
    int vDotIndex = pKernelName.find(".");
    return pKernelName.substr(0, vDotIndex);
}

std::vector<std::string> GetKernelsSourceFiles(std::string pKernelsFolder)
{
    //std::cout << "Kernels Folder:" << pKernelsFolder << '\n' << '\n';

    DIR *vDirectory;

    std::string vDot (".");
    std::string vDotDot ("..");
    std::vector<std::string> vFiles;

    if ((vDirectory = opendir(pKernelsFolder.c_str())) != NULL) 
    {
        struct dirent *vFile;

        /* print all the files and directories within directory */
        while ((vFile = readdir (vDirectory)) != NULL) 
        {
            if (vDot.compare(vFile->d_name) != 0 && vDotDot.compare(vFile->d_name) != 0)
            {
                //printf ("%s\n", vFile->d_name);
                vFiles.push_back(vFile->d_name);          
            }
        }
        closedir (vDirectory);
    }

    return vFiles;
}

std::string GetKernelSourceCode(std::string pFile)
{
    std::ifstream vSourceFile(pFile);

    std::string vSourceCode(std::istreambuf_iterator<char>(vSourceFile),(std::istreambuf_iterator<char>()));

    //std::cout << vSourceCode << "\n";

    return vSourceCode;
}

cl::Program CompileKernelProgram(std::string pSourceCode)
{
    const char* vSourceCode = pSourceCode.c_str();

    cl::Program::Sources vSources(1, std::make_pair( vSourceCode, pSourceCode.length()+1));
	cl::Program vProgram(gContext, vSources);

	vProgram.build(gDevices);

    return vProgram;
}

void StoreKernelProgram(std::string pKernelName, cl::Program pKernelProgram)
{
    std::unordered_map<std::string,cl::Program>::const_iterator vIter = gProgrmasList.find(pKernelName);
	if(vIter == gProgrmasList.end())
	{
        gProgrmasList[pKernelName] = pKernelProgram;
	}
}

#pragma endregion

#pragma region Jni utility
std::string GetStringFromJavaString(JNIEnv *pEnv, jstring pString)
{
    return pEnv->GetStringUTFChars(pString, NULL);
}

void ReleaseJStringResources(JNIEnv *pEnv, jstring pJavaString, std::string pCppString)
{
	pEnv->ReleaseStringUTFChars(pJavaString, pCppString.c_str());
}
#pragma endregion