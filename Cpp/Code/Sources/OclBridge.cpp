#define __CL_ENABLE_EXCEPTIONS
#include "../Headers/org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge.h"
#include "../Headers/OclUtility.h"
#include "../Headers/JniUtility.h"
#include <fstream>
#include <iostream>
#include <dirent.h>
#include <iomanip>
#include <unordered_map>
#include <chrono>
#include <math.h> 

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

void PrintClError(cl::Error& error, const int line)
{   
	std::cout << "Error" << std::endl;
	std::cout << error.what() << "(" << error.err() << ")" << " - line: " << line << std::endl;
}

#pragma region StopWatch
class Stopwatch{
public:
   enum TimeFormat{ NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS };

   Stopwatch(): start_time(), laps({}) {
      start();
   }

   void start(){
      start_time = std::chrono::high_resolution_clock::now();
      laps = {start_time};
   }

   template<TimeFormat fmt = TimeFormat::MILLISECONDS>
   std::uint64_t lap(){
      const auto t = std::chrono::high_resolution_clock::now();
      const auto last_r = laps.back();
      laps.push_back( t );
      return ticks<fmt>(last_r, t);
   }

   template<TimeFormat fmt = TimeFormat::MILLISECONDS>
   std::uint64_t elapsed(){
      const auto end_time = std::chrono::high_resolution_clock::now();
      return ticks<fmt>(start_time, end_time);
   }

   template<TimeFormat fmt_total = TimeFormat::MILLISECONDS, TimeFormat fmt_lap = fmt_total>
   std::pair<std::uint64_t, std::vector<std::uint64_t>> elapsed_laps(){
      std::vector<std::uint64_t> lap_times;
      lap_times.reserve(laps.size()-1);

      for( std::size_t idx = 0; idx <= laps.size()-2; idx++){
         const auto lap_end = laps[idx+1];
         const auto lap_start = laps[idx];
         lap_times.push_back( ticks<fmt_lap>(lap_start, lap_end) );
      }

      return { ticks<fmt_total>(start_time, laps.back()), lap_times };
   }

private:
   typedef std::chrono::time_point<std::chrono::high_resolution_clock> time_pt;
   time_pt start_time;
   std::vector<time_pt> laps;

   template<TimeFormat fmt = TimeFormat::MILLISECONDS>
   static std::uint64_t ticks( const time_pt& start_time, const time_pt& end_time){
      const auto duration = end_time - start_time;
      const std::uint64_t ns_count = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

      switch(fmt){
      case TimeFormat::NANOSECONDS:
      {
         return ns_count;
      }
      case TimeFormat::MICROSECONDS:
      {
         std::uint64_t up = ((ns_count/100)%10 >= 5) ? 1 : 0;
         const auto mus_count = (ns_count /1000) + up;
         return mus_count;
      }
      case TimeFormat::MILLISECONDS:
      {
         std::uint64_t up = ((ns_count/100000)%10 >= 5) ? 1 : 0;
         const auto ms_count = (ns_count /1000000) + up;
         return ms_count;
      }
      case TimeFormat::SECONDS:
      {
         std::uint64_t up = ((ns_count/100000000)%10 >= 5) ? 1 : 0;
         const auto s_count = (ns_count /1000000000) + up;
         return s_count;
      }
      }
    }
};

constexpr Stopwatch::TimeFormat nanoseconds = Stopwatch::TimeFormat::NANOSECONDS;
constexpr Stopwatch::TimeFormat microseconds = Stopwatch::TimeFormat::MICROSECONDS;
constexpr Stopwatch::TimeFormat milliseconds = Stopwatch::TimeFormat::MILLISECONDS;
constexpr Stopwatch::TimeFormat seconds = Stopwatch::TimeFormat::SECONDS;


std::string show_times( const std::vector<std::uint64_t>& times ){
    std::string result("{");
    for( const auto& t : times ){
        result += std::to_string(t) + ",";
    }
    result.back() = static_cast<char>('}');
    return result;
}
#pragma endregion

std::unordered_map<std::string, std::uint64_t[2]> gProgrmasProfiling;

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

#define printError(error) PrintClError(error, __LINE__)

#pragma region dispose definition

void DisposePrograms();
void DisposeDevices();
void DisposeCommandQueue();
void DisposeContext();

#pragma endregion

#pragma region Classes

class OclKernelExecutionInfo
{
    protected:

        JNIEnv *mEnv;
        jobject mObj;
        std::string mKernelName;

        unsigned char *mStream, *mResult;
        int* mIndexes;

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
            return this->mStreamSize;
        }
        int GetStreamLength()
        {
            return this->mStreamLength;
        }


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
    private: 
        int mWorkGroupSize;

        int mLocalCacheLength;
        unsigned char *mLocalCache;
        size_t mLocalCacheSize;

        int mIdentityLength;
        unsigned char *mIdentity;
        size_t mIdendtitySize;

        int mMidResultLength;
        size_t mMidResultSize;
        unsigned char *mMidResult;
    protected:
        void SetUpResult()
        {
            mResultLength = mInfoLength + mOutputTupleDimension;
            mResultSize = sizeof(unsigned char) * mResultLength;
            mResult = new unsigned char[mResultLength];
        }
        OclReduceExecutionInfo* SetUpInfoLength()
        {
            mInfoLength = 1 + (int)mStream[0];
            return this;
        }
        OclReduceExecutionInfo* SetUpMidResult()
        {
            // std::cout << "arity - " << (int)mStream[0] << std::endl;
            // std::cout << "mInfoLength - " << mInfoLength << std::endl;

            mMidResultLength = this->GetStreamLength() - mInfoLength;
            mMidResultSize = sizeof(unsigned char) * mMidResultLength;
            mMidResult = new unsigned char[mMidResultLength];
            return this;
        }
        OclReduceExecutionInfo* SetUpIdentityAndLocalCache(jbyteArray pIdentity)
        {
            mIdentityLength = mEnv->GetArrayLength(pIdentity);
            mIdentity = (unsigned char *)mEnv->GetByteArrayElements(pIdentity, 0);
            mIdendtitySize = sizeof(unsigned char) * mIdentityLength;

            mLocalCacheLength = mWorkGroupSize * mOutputTupleDimension;
            mLocalCacheSize = sizeof(unsigned char) * mLocalCacheLength;
            mLocalCache = new unsigned char[mLocalCacheLength];

            // for(int i = 0; i < mLocalCacheLength; i++)
            // {
            //     for(int j = 0; j < mIdentityLength; j++)
            //     {
            //         mLocalCache[i] = mIdentity[i];
            //     }
            // }
            return this;
        }
    public:
        OclReduceExecutionInfo (JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
                                jint pOutputTupleDimension, jbyteArray pIdentity, jint pWorkGroupSize)
            : OclKernelExecutionInfoForOutputTuple (pEnv, pObj, pKernelName, pStream, pIndexes, pOutputTupleDimension, pIdentity)
        {
            mWorkGroupSize = pWorkGroupSize;
            this->SetUpInfoLength()->SetUpMidResult()->SetUpIdentityAndLocalCache(pIdentity)->SetUpResult();
        }

        #pragma region getters

        int GetWorkGroupSize()
        {
            return mWorkGroupSize;
        }

        unsigned char* GetLocalCache()
        {
            return mLocalCache;
        }

        size_t GetLocalCacheSize()
        {
            return mLocalCacheSize;
        }

        int GetLocalCacheLength()
        {
            return mLocalCacheLength;
        }

        unsigned char* GetIdentity()
        {
            return mIdentity;
        }

        size_t GetIdentitySize()
        {
            return mIdendtitySize;
        }

        int GetIdentityLength()
        {
            return mIdentityLength;
        }

        unsigned char* GetMidResult()
        {
            return mMidResult;
        }

        size_t GetMidResultSize()
        {
            return mMidResultSize;
        }
        int GetMidResultLength()
        {
            return mMidResultLength;
        }
        unsigned char* GetResult()
        {
            for(int i = 0; i < mInfoLength; i++)
            {
                mResult[i] = mStream[i];
            }
            return mResult;
        }
        #pragma endregion
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

        
        cl::Buffer vStreamBuffer(gContext, CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, vStreamSize, pKernelInfo->GetStream());
        cl::Buffer vIndexesBuffer(gContext, CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, vIndexesSize, pKernelInfo->GetIndexes());
        cl::Buffer vResultBuffer(gContext, CL_MEM_WRITE_ONLY, vResultSize);

        // gCommandQueue.enqueueWriteBuffer(vStreamBuffer, CL_TRUE, 0, vStreamSize, pKernelInfo->GetStream());
        // gCommandQueue.enqueueWriteBuffer(vIndexesBuffer, CL_TRUE, 0, vIndexesSize, pKernelInfo->GetIndexes());

        // std::cout << "Set arg: StreamBuffer" << std::endl;
        vKernel.setArg(vArgIndex++, vStreamBuffer);
        // std::cout << "Set arg: IndexesBuffer" << std::endl;
        vKernel.setArg(vArgIndex++, vIndexesBuffer);
        // std::cout << "Set arg: ResultBuffer" << std::endl;
        vKernel.setArg(vArgIndex++, vResultBuffer);

        vKernel.setArg(vArgIndex++, pKernelInfo->GetIndexesLength());

        // std::cout << "StreamBufferSize: " << vStreamSize << std::endl;
        // std::cout << "IndexesBuffer: " << vIndexesSize << std::endl;
        // std::cout << "ResultBuffer: " << vResultSize << std::endl;

        cl::NDRange global(pKernelInfo->GetIndexesLength());

        gCommandQueue.enqueueNDRangeKernel(vKernel, cl::NullRange, global, cl::NullRange);

        gCommandQueue.enqueueReadBuffer(vResultBuffer, CL_TRUE, 0, vResultSize, pKernelInfo->GetResult());
    }
    catch(cl::Error& vError)
    {
        printError(vError);
        exit(1);
    }
}

void RunKernel(OclReduceExecutionInfo* pKernelInfo)
{
    size_t vStreamSize, vIndexesSize, vResultSize, vLocalCacheSize, vIdentiySize, vMidResultSize;
    vStreamSize = pKernelInfo->GetStreamSize();
    vIndexesSize = pKernelInfo->GetIndexesSize();
    vResultSize = pKernelInfo->GetResultSize();
    vIdentiySize = pKernelInfo->GetIdentitySize();
    vMidResultSize = pKernelInfo->GetMidResultSize();
    std::vector<cl::Buffer> vBuffers;
    int vSteps = ceil(log2((double)pKernelInfo->GetIndexesLength())/log2((double)pKernelInfo->GetWorkGroupSize()));
    int vCurrentStep = 0;
    // std::cout << "idexes length: " << pKernelInfo->GetIndexesLength();
    // std::cout << "wk size: " << pKernelInfo->GetWorkGroupSize() << " - Steps: " << vSteps << std::endl;
    try
    {
        int vArgIndex = 0;

        std::string vKernelNameStep0(pKernelInfo->GetKernelName());
        vKernelNameStep0.append("_step0");
        std::string vKernelNameStep1(pKernelInfo->GetKernelName());
        vKernelNameStep1.append("_step1");


        cl::Kernel vKernelStep0 = cl::Kernel(gProgrmasList[vKernelNameStep0], vKernelNameStep0.c_str());
        cl::Kernel vKernelStep1 = cl::Kernel(gProgrmasList[vKernelNameStep1], vKernelNameStep1.c_str());


        cl::Buffer vStreamBuffer(gContext, CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, vStreamSize, pKernelInfo->GetStream());
        cl::Buffer vIndexesBuffer(gContext, CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, vIndexesSize, pKernelInfo->GetIndexes());
        cl::Buffer vResultBuffer(gContext, CL_MEM_WRITE_ONLY, vResultSize);

        cl::Buffer vIdendityBuffer(gContext, CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, vIdentiySize, pKernelInfo->GetIdentity());;
        cl::Buffer vMidResultBuffer(gContext,  CL_MEM_READ_WRITE | CL_MEM_COPY_HOST_PTR, vMidResultSize, pKernelInfo->GetMidResult());
        

        // gCommandQueue.enqueueWriteBuffer(vStreamBuffer, CL_TRUE, 0, vStreamSize, pKernelInfo->GetStream());
        // gCommandQueue.enqueueWriteBuffer(vIndexesBuffer, CL_TRUE, 0, vIndexesSize, pKernelInfo->GetIndexes());
        // gCommandQueue.enqueueWriteBuffer(vIdendityBuffer, CL_TRUE, 0, vIdentiySize, pKernelInfo->GetIdentity());

        // std::cout << "Set arg: StreamBuffer" << std::endl;
        vKernelStep0.setArg(vArgIndex++, vStreamBuffer);
        //std::cout << "Ok" << std::endl;
        // std::cout << "Set arg: IndexesBuffer" << std::endl;
        vKernelStep0.setArg(vArgIndex++, vIndexesBuffer);
        // std::cout << "Ok" << std::endl;
        // std::cout << "Set arg: ResultBuffer" << std::endl;
        // vKernelStep0.setArg(vArgIndex++, vResultBuffer);
        //std::cout << "Ok" << std::endl;

        // std::cout << "Set arg: MidResultBuffer" << std::endl;
        vKernelStep0.setArg(vArgIndex++, vMidResultBuffer);
        //std::cout << "Ok" << std::endl;

        // std::cout << "Set arg: IdendityBuffer" << std::endl;
        vKernelStep0.setArg(vArgIndex++, vIdendityBuffer);
        //std::cout << "Ok" << std::endl;

        std::cout << std::endl;
        // std::cout << "Set arg: LocalCacheBuffer" << std::endl;
        // std::cout << "unsigned char - " << sizeof(unsigned char) << std::endl;
        // std::cout << "length - " << pKernelInfo->GetLocalCacheLength() << std::endl;
        // std::cout << "size - " << pKernelInfo->GetLocalCacheSize() << std::endl;
        // std::cout << "stream - " << pKernelInfo->GetStreamLength() << std::endl;
        // std::cout << "finalResult - " << pKernelInfo->GetResultLength() << std::endl;
        // std::cout << "midResult - " << pKernelInfo->GetMidResultLength() << std::endl;
        vKernelStep0.setArg(vArgIndex++, cl::Local(pKernelInfo->GetLocalCacheSize()));
        //std::cout << "Ok" << std::endl;


        // std::cout << "stream - " << pKernelInfo->GetIndexesLength() << std::endl;
        cl::NDRange vGlobal(vSteps * pKernelInfo->GetWorkGroupSize());
        cl::NDRange vLocal(pKernelInfo->GetWorkGroupSize());

        gCommandQueue.enqueueNDRangeKernel(vKernelStep0, cl::NullRange, vGlobal, vLocal);

        vCurrentStep++;

        int vCurrentGlobalSize = pKernelInfo->GetIndexesLength();
        do
        {   
            vArgIndex = 0;
            vCurrentGlobalSize = vSteps * pKernelInfo->GetWorkGroupSize();
            cl::NDRange vNewGlobal(vCurrentGlobalSize);

            vKernelStep1.setArg(vArgIndex++, vMidResultBuffer);

            vKernelStep1.setArg(vArgIndex++, vIdendityBuffer);

            vKernelStep1.setArg(vArgIndex++, vResultBuffer);

            vKernelStep1.setArg(vArgIndex++, cl::Local(pKernelInfo->GetLocalCacheSize()));

            vCurrentStep++;
            vKernelStep1.setArg(vArgIndex++, vSteps - vCurrentStep);

            gCommandQueue.enqueueNDRangeKernel(vKernelStep1, cl::NullRange, vNewGlobal , vLocal);
            
        } while (vCurrentStep < vSteps);
        

        gCommandQueue.enqueueReadBuffer(vResultBuffer, CL_TRUE, 0, vResultSize, pKernelInfo->GetResult());
    }
    catch(cl::Error& vError)
    {
        printError(vError);
        exit(1);
    }
}


JNIEXPORT void Java_org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge_ListDevices(JNIEnv *pEnv, jobject pObj)
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

JNIEXPORT void Java_org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge_Initialize(JNIEnv *pEnv, jobject pObj, jstring pKernelsFolder)
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
    catch(cl::Error& vError)
    {
        printError(vError);
        exit(1);
    }
}

JNIEXPORT void Java_org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge_Dispose(JNIEnv *pEnv, jobject pObj)
{
    DisposePrograms();
    DisposeDevices();
    
    DisposeCommandQueue();
    DisposeContext();
}

JNIEXPORT jlongArray JNICALL Java_org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge_GetKernelProfiling(
    JNIEnv *pEnv, jobject pObj, jstring pKernelName)
{
    std::string vKernelName = GetStringFromJavaString(pEnv, pKernelName);

    jlongArray vRet = pEnv->NewLongArray(2);
    pEnv->SetLongArrayRegion(vRet, 0, 2, GetKernelElapsed(vKernelName));
    return vRet;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge_OclMap(
    JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
    jint pOutputTupleDimension, jbyteArray pOutputTupleInfo)
{
    Stopwatch vJavaToCWatch;

    OclKernelExecutionInfo *vKernelInfo = new OclMapExecutionInfo(pEnv, pObj, pKernelName, pStream, pIndexes, pOutputTupleDimension, pOutputTupleInfo);

    std::uint64_t javaToCElapsed = vJavaToCWatch.elapsed<nanoseconds>();
    SetJavaToCElapsed(vKernelInfo->GetKernelName(), javaToCElapsed);


    Stopwatch vKernelExecutionWatch;

    RunKernel(vKernelInfo);

    std::uint64_t kernelExexutionElapsed = vKernelExecutionWatch.elapsed<nanoseconds>();
    SetComputationElapsed(vKernelInfo->GetKernelName(), javaToCElapsed);

    jbyteArray vRet = pEnv->NewByteArray(vKernelInfo->GetResultLength());
	pEnv->SetByteArrayRegion(vRet, 0, vKernelInfo->GetResultLength(), (signed char *)vKernelInfo->GetResult());

    return vRet;
}

JNIEXPORT jbooleanArray JNICALL 
Java_org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge_OclFilter(
    JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes)
{
    // std::string vKernelName = GetStringFromJavaString(pEnv, pKernelName);

    // unsigned char* vStream;
    // int* vIndexes;

    // int vStreamLength, vIndexesLegth, vResultLength;
    // size_t vStreamSize, vIndexesSize, vResultSize;
    Stopwatch vJavaToCWatch;

    OclFilterExecutionInfo *vKernelInfo = new OclFilterExecutionInfo(pEnv, pObj, pKernelName, pStream, pIndexes);

    std::uint64_t javaToCElapsed = vJavaToCWatch.elapsed<nanoseconds>();
    SetJavaToCElapsed(vKernelInfo->GetKernelName(), javaToCElapsed);

    Stopwatch vKernelExecutionWatch;

    RunKernel(vKernelInfo);

    std::uint64_t kernelExexutionElapsed = vKernelExecutionWatch.elapsed<nanoseconds>();
    SetComputationElapsed(vKernelInfo->GetKernelName(), javaToCElapsed);

    jbooleanArray vRet = pEnv->NewBooleanArray(vKernelInfo->GetResultLength());
	pEnv->SetBooleanArrayRegion(vRet, 0, vKernelInfo->GetResultLength(), vKernelInfo->GetResult());

    return vRet;
}

JNIEXPORT jbyteArray JNICALL 
Java_org_apache_flink_streaming_api_ocl_bridge_AbstractOclBridge_OclReduce(
    JNIEnv *pEnv, jobject pObj, jstring pKernelName, jbyteArray pStream, jintArray pIndexes, 
    jint pOutputTupleDimension, jbyteArray pIdentity, jint pWorkGroupSize)
{

    OclReduceExecutionInfo *vKernelInfo 
        = new OclReduceExecutionInfo(pEnv, pObj, pKernelName, pStream, pIndexes, pOutputTupleDimension, pIdentity, pWorkGroupSize);

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
    gProgrmasList.clear();
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

    std::cout << "compiling kernel: " << vFullName << std::endl;

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

    try
    {
	    vProgram.build(gDevices);
    }
    catch(cl::Error& vError)
    {
        std::cout << "build error" << std::endl;
        std::cout << "devices: " << gDevices.size() << std::endl;
        // if (vError->err() == CL_BUILD_PROGRAM_FAILURE || strcmp(vError->what(),"clBuildProgram"))
        // {
        for (cl::Device vDevice : gDevices)
        {
            std::cout << "device" << vDevice.getInfo<CL_DEVICE_NAME>() << std::endl;
            // Check the build status
            cl_build_status vStatus = vProgram.getBuildInfo<CL_PROGRAM_BUILD_STATUS>(vDevice);
            if (vStatus != CL_BUILD_ERROR)
                continue;

            // Get the build log
            std::string vName     = vDevice.getInfo<CL_DEVICE_NAME>();
            std::string vBuildlog = vProgram.getBuildInfo<CL_PROGRAM_BUILD_LOG>(vDevice);
            std::cout << "Build log for " << vName << ":" << std::endl
                        << vBuildlog << std::endl;
        }
        throw vError;
        // }
        // else
        // {
        //     throw vError;
        // }
    }
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

void SetJavaToCElapsed(std::string pKernelName, std::uint64_t elapsedNanosconds)
{
    std::unordered_map<std::string,std::uint64_t[2]>::const_iterator vIter = gProgrmasProfiling.find(pKernelName);
    if (vIter == gProgrmasProfiling.end())
    {
        gProgrmasProfiling[pKernelName][0] = elapsedNanosconds;
    }
    else
    {
        gProgrmasProfiling[pKernelName][0] = elapsedNanosconds;
    }
}

void SetComputationElapsed(std::string pKernelName, std::uint64_t elapsedNanosconds)
{
    std::unordered_map<std::string,std::uint64_t[2]>::const_iterator vIter = gProgrmasProfiling.find(pKernelName);
    if (vIter == gProgrmasProfiling.end())
    {
        gProgrmasProfiling[pKernelName][1] = elapsedNanosconds;
    }
    else
    {
        gProgrmasProfiling[pKernelName][1] = elapsedNanosconds;
    }
    
}

long* GetKernelElapsed(std::string pKernelName)
{
    return (long *)gProgrmasProfiling[pKernelName];
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