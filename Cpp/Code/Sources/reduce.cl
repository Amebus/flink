void fillStringWith(int si,int sl, char c, char *s)
{
    for (; si < sl; si++) {
        s[si] = c;
    }
}

void integerToString(int n, char *s, int sl)
{
    fillStringWith(0, sl, '\0', s);
    char const digit[] = "0123456789";
    char* p = s;
    if(n<0){
        *p++ = '-';
        n *= -1;
    }
    int shifter = n;
    do{
        ++p;
        shifter = shifter/10;
    }while(shifter);
    *p = '\0';
    do{
        *--p = digit[n%10];
        n = n/10;
    }while(n);
}

int stringToInteger(char *s)
{
    const char z = '0';
    int r = 0, st = 0, p = 1;
    
    while(s[st] != '\0')
    {
        st++;
    }
    for(int i = st-1; i >= 0 && s[i] != '-' ; i--)
    {
        r+=((s[i]-z)*p);
        p*=10;
    }
    if(s[0]=='-')
    {
        r*=-1;
    }
    return r;
}

int localStringToInteger(__local unsigned char *s)
{
    const char z = '0';
    int r = 0, st = 0, p = 1;
    
    while(s[st] != '\0')
    {
        st++;
    }
    for(int i = st-1; i >= 0 && s[i] != '-' ; i--)
    {
        r+=((s[i]-z)*p);
        p*=10;
    }
    if(s[0]=='-')
    {
        r*=-1;
    }
    return r;
}


#define SER_INT(i, si, r)          \
        r[si] = (i >> 24) & 0xFF;  \
        si++;                      \
        r[si] = (i >> 16) & 0xFF;  \
        si++;                      \
        r[si] = (i >> 8) & 0xFF;   \
        si++;                      \
        r[si] = i & 0xFF;          \

#define SER_DOUBLE(d, si, r, t)     \
        t = (long) d;               \
        r[si] = (t >> 56) & 0xFF;   \
        si++;                       \
        r[si] = (t >> 48) & 0xFF;   \
        si++;                       \
        r[si] = (t >> 40) & 0xFF;   \
        si++;                       \
        r[si] = (t >> 32) & 0xFF;   \
        si++;                       \
        r[si] = (t >> 24) & 0xFF;   \
        si++;                       \
        r[si] = (t >> 16) & 0xFF;   \
        si++;                       \
        r[si] = (t >> 8) & 0xFF;    \
        si++;                       \
        r[si] = t & 0xFF;           \

#define SER_STRING(s, si, l, r)                \
        SER_INT(l, si, r);                     \
        si++;                                  \
        for(int _ii = 0; _ii < l; _ii++, si++) \
        {                                      \
            r[si] = s[_ii];                    \
        }                                      \

#define DESER_INT(d, si, r) \
            r <<= 8;        \
            r |= d[si];     \
            si++;           \
            r <<= 8;        \
            r |= d[si];     \
            si++;           \
            r <<= 8;        \
            r |= d[si];     \
            si++;           \
            r <<= 8;        \
            r |= d[si];     \

#define DESER_DOUBLE(d, si, r, t)   \
            t <<= 8;                \
            t |= d[si];             \
            si++;                   \
            t <<= 8;                \
            t |= d[si];             \
            si++;                   \
            t <<= 8;                \
            t |= d[si];             \
            si++;                   \
            t <<= 8;                \
            t |= d[si];             \
            si++;                   \
            t <<= 8;                \
            t |= d[si];             \
            si++;                   \
            t <<= 8;                \
            t |= d[si];             \
            si++;                   \
            t <<= 8;                \
            t |= d[si];             \
            si++;                   \
            t <<= 8;                \
            t |= d[si];             \
            r = *((double*)t);      \

#define DESER_STRING(d, si, rs, ri) 			    \
            DESER_INT(d, si, ri);   			    \
            si++;                   			    \
            rs = (__local unsigned char *)&d[si]; 	\
            si+=ri;                 			    \

#define INT 1
#define DOUBLE 2
#define STRING 3
#define LAST_STEP 1

#define STRING_MAX_BYTE_1 1
#define STRING_MAX_BYTE_2 1

__kernel void mapStringToInt(
	__global unsigned char *_data, 
	__global int *_dataIndexes, 
    __global unsigned char *_finalResult,
	__global unsigned char *_identity,
    __global unsigned char *_midResults,
    __local unsigned char *_localCache)
{
    
    // region variables to add
    uint _grSize = get_local_size(0);
    uint _gSize = get_global_size(0);
    uint _outputCount = get_num_groups(0);
    uint _steps = ceil(log2((double)_gSize)/log2((double)_grSize));
    
    uint _lId = get_local_id(0);
    uint _grId = get_group_id(0);
    //
    uint _gId = get_global_id(0);
    unsigned char _arity = 3;
    int _i = _dataIndexes[_gId];
    int _userIndex = _i;
    long _l = 0;

    int _roff = 2;
    int _otd = 4;
    int _ri0 = _roff + _grId * _otd; //Deve cambiare, _gId -> _grId

    // region variables to add
    uint _lri0 = _lId * _otd;
    uint _lri1 = _lri0 + getMaxByte();
    uint _uiTemp = 0;
    int _iTemp = 0;
    unsigned char _tCounter = 0;
    bool _continueCopy = 1;
    uint _copyLength = 0;
    //

    int _r0;

    int _sl0;
    __local unsigned char* _t0;

    //copy from global to local
    unsigned char _types[3];
    _types[0] = _data[1];
    _types[1] = _data[2];
    _types[2] = _data[3];
    for(int i = _i, k = _lId * _otd, j = 0; _tCounter < _arity; _tCounter++)
    {
        if(_types[_tCounter] < STRING)
        {
            _copyLength = 4;
            if(_types[_tCounter] == DOUBLE)
            {
                _copyLength = 8;
            }
            
            while(j < _copyLength)
            {
                _localCache[k++] = _data[i++];
                j++;
            }
            j = 0;
        }
        else
        {
            i+=4;
            do
            {
                _localCache[k] = _data[i++];
                _continueCopy = _localCache[k] != '\0';
                k++;
            } while(_continueCopy);

            int _sMaxBytes = 0;
            //if to understand the string max length
            if(_tCounter == 1)
            {
                _sMaxBytes = STRING_MAX_BYTE_1;
            }
            else
            {
                _sMaxBytes = STRING_MAX_BYTE_2;
            }

            if(k < _sMaxBytes)
            {
                for(;k < _sMaxBytes; k++)
                {
                    _localCache[k] = '\0';
                }
            }
        }
        _continueCopy = 1;
    }

    barrier(CLK_LOCAL_MEM_FENCE);

    for(uint _currentStep = _steps; _currentStep > 0 && _grId < _outputCount; _currentStep--)
    {
        _outputCount = ceil((double)_outputCount/_grSize);

        for(uint _stride = _grSize/2; _stride > 0; _stride /= 2)
        {
            if(_lId < _stride)
            {
                //dentro al ciclo con lo stride
                _iTemp = _i;
                DESER_STRING( _localCache, _iTemp, _t0, _sl0 );

                //funzione utente
                _r0 = localStringToInteger(_t0);


                _iTemp = _ri0;
                SER_INT( _r0, _iTemp, _localCache );
            }   
            barrier(CLK_LOCAL_MEM_FENCE);
        }

        if(_currentStep > LAST_STEP)
        {
            for(uint i = 0, j = _gId; i < _otd; i++, j++)
            {
                _midResults[j] = _identity[i];
            }
            barrier(CLK_GLOBAL_MEM_FENCE);

            for(uint i = 0, j = _grId; i < _otd; i++, j++)
            {
                _midResults[j] = _localCache[i];
            }
            barrier(CLK_GLOBAL_MEM_FENCE);

        }
        else
        {
            barrier(CLK_LOCAL_MEM_FENCE);
        }

        if(_grId < _outputCount)
        {
            for(uint i = 0, j = _gId, k = _lId * _otd; i < _otd; i++, j++, k++)
            {
                _localCache[k] = _midResults[j];
            }
            barrier(CLK_LOCAL_MEM_FENCE);
        }
    }

    //copia risultato
    if(_gId == 0)
    {
        for(int i = 0, j = _ri0; i < _otd; i++, j++)
        {
            _finalResult[j] = _localCache[i];
        }
    }
};
