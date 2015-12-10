#include "redis_client.hpp"
#include <iostream>
#include <functional>

#define BIND_INT(val) std::bind(&FetchInteger, std::placeholders::_1, val)
#define BIND_STR(val) std::bind(&FetchString, std::placeholders::_1, val)
#define BIND_STU(val) std::bind(&FetchStatus, std::placeholders::_1, val)
#define BIND_VINT(val) std::bind(&FetchIntegerArray, std::placeholders::_1, val)
#define BIND_VSTR(val) std::bind(&FetchStringArray, std::placeholders::_1, val)
#define BIND_SLOT(val) std::bind(&FetchSlot, std::placeholders::_1, val)

// crc16 for computing redis cluster slot
static const uint16_t crc16Table[256] =
{
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

uint16_t CRC16(const char *pszData, int nLen)
{
    int nCounter;
    uint16_t nCrc = 0;
    for (nCounter = 0; nCounter < nLen; nCounter++)
        nCrc = (nCrc << 8) ^ crc16Table[((nCrc >> 8) ^ * pszData++) & 0x00FF];
    return nCrc;
}

uint32_t HASH_SLOT(const std::string &strKey)
{
    const char *pszKey = strKey.data();
    size_t nKeyLen = strKey.size();
    size_t nStart, nEnd; /* start-end indexes of { and  } */

    /* Search the first occurrence of '{'. */
    for (nStart = 0; nStart < nKeyLen; nStart++)
        if (pszKey[nStart] == '{')
            break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (nStart == nKeyLen)
        return CRC16(pszKey, nKeyLen) & 16383;

    /* '{' found? Check if we have the corresponding '}'. */
    for (nEnd = nStart + 1; nEnd < nKeyLen; nEnd++)
        if (pszKey[nEnd] == '}')
            break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (nEnd == nKeyLen || nEnd == nStart + 1)
        return CRC16(pszKey, nKeyLen) & 16383;

    /* If we are here there is both a { and a  } on its right. Hash
     * what is in the middle between { and  }. */
    return CRC16(pszKey + nStart + 1, nEnd - nStart - 1) & 16383;
}

static inline std::ostream & operator<<(std::ostream &os, const std::pair<int, redisReply *> &pairReply)
{
    int nLevel = pairReply.first;
    redisReply *pReply = pairReply.second;

    for (int i = 0; i < nLevel; ++i)
        os << "  ";

    if (!pReply)
        os << "nullptr" << std::endl;
    else if (pReply->type == REDIS_REPLY_INTEGER)
        os << "integer: " << pReply->integer << std::endl;
    else if (pReply->type == REDIS_REPLY_STRING)
        os << "string: " << pReply->str << std::endl;
    else if (pReply->type == REDIS_REPLY_ARRAY)
    {
        os <<  "array: " << std::endl;
        for (int j = 0; j < pReply->elements; ++j)
            os << std::make_pair(nLevel + 1, pReply->element[j]);
    }
    else if (pReply->type == REDIS_REPLY_STATUS)
        os << "status: " << pReply->str << std::endl;
    else if (pReply->type == REDIS_REPLY_ERROR)
        os << "error: " << pReply->str << std::endl;
    else if (pReply->type == REDIS_REPLY_NIL)
        os << "nil" << std::endl;
    else
        os << "unknown" << std::endl;
    return os;
}

static inline std::ostream & operator<<(std::ostream &os, redisReply *pReply)
{
    return os << std::make_pair(0, pReply);
}

// for finding matched slot/server with binary search
bool operator< (const SlotRegion &lReg, const SlotRegion &rReg)
{
    return lReg.nStartSlot < rReg.nStartSlot;
}

class CompSlot
{
public:
    bool operator()(const SlotRegion &slotReg, int nSlot) { return slotReg.nEndSlot < nSlot; }
    bool operator()(int nSlot, const SlotRegion &slotReg) { return nSlot < slotReg.nStartSlot; }
};

class IntResConv
{
public:
    IntResConv(int nConvRet = RC_KEY_NOT_EXIST, long nVal = 0) : m_nConvRet(nConvRet), m_nVal(nVal) {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && pReply->integer == m_nVal)
            return m_nConvRet;
        return nRet;
    }

private:
    int m_nConvRet;
    long m_nVal;
};

class StuResConv
{
public:
    StuResConv(const std::string &strVal) : m_strVal(strVal) {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && m_strVal != pReply->str)
            return RC_REPLY_ERR;
        return nRet;
    }

private:
    std::string m_strVal;
};

static inline int FetchInteger(redisReply *pReply, long *pnVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        if (pnVal)
            *pnVal = pReply->integer;
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
        return RC_REPLY_ERR;
    else if (pReply->type == REDIS_REPLY_NIL)
        return RC_KEY_NOT_EXIST;
    else
        return RC_TYPE_ERR;
}

static inline int FetchString(redisReply *pReply, std::string *pstrVal)
{
    if (pReply->type == REDIS_REPLY_STRING)
    {
        if (pstrVal)
            pstrVal->assign(pReply->str, pReply->len);
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
        return RC_REPLY_ERR;
    else if (pReply->type == REDIS_REPLY_NIL)
        return RC_KEY_NOT_EXIST;
    else
        return RC_TYPE_ERR;
}

static inline int FetchStatus(redisReply *pReply, std::string *pstrVal)
{
    if (pReply->type == REDIS_REPLY_STATUS)
    {
        if (pstrVal)
            pstrVal->assign(pReply->str, pReply->len);
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
        return RC_REPLY_ERR;
    else if (pReply->type == REDIS_REPLY_NIL)
        return RC_KEY_NOT_EXIST;
    else
        return RC_TYPE_ERR;
}

static inline int FetchIntegerArray(redisReply *pReply, std::vector<long> *pvecLongVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        int nRet = RC_SUCCESS;
        if (!pvecLongVal)
            return nRet;

        long nVal;
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            int nSubRet = FetchInteger(pReply->element[i], &nVal);
            if (nSubRet == RC_SUCCESS)
                pvecLongVal->push_back(nVal);
            else
                nRet = RC_PART_SUCCESS;
        }
        return nRet;
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
        return RC_REPLY_ERR;
    else if (pReply->type == REDIS_REPLY_NIL)
        return RC_KEY_NOT_EXIST;
    else
        return RC_TYPE_ERR;
}

static inline int FetchStringArray(redisReply *pReply, std::vector<std::string> *pvecStrVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        int nRet = RC_SUCCESS;
        if (!pvecStrVal)
            return nRet;

        std::string strVal;
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            int nSubRet = FetchString(pReply->element[i], &strVal);
            if (nSubRet == RC_SUCCESS)
                pvecStrVal->push_back(strVal);
            else if (nSubRet == RC_KEY_NOT_EXIST)
            {
                pvecStrVal->push_back("");
                nRet = RC_PART_SUCCESS;
            }
            else
                return nSubRet;
        }
        return nRet;
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
        return RC_REPLY_ERR;
    else if (pReply->type == REDIS_REPLY_NIL)
        return RC_KEY_NOT_EXIST;
    else
        return RC_TYPE_ERR;
}

static inline int FetchSlot(redisReply *pReply, std::vector<SlotRegion> *pvecSlot)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (!pvecSlot)
            return RC_SUCCESS;

        SlotRegion slotReg;
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            redisReply *pSubReply = pReply->element[i];
            if (pSubReply->type != REDIS_REPLY_ARRAY || pSubReply->elements < 3)
                return RC_TYPE_ERR;

            slotReg.nStartSlot = pSubReply->element[0]->integer;
            slotReg.nEndSlot = pSubReply->element[1]->integer;
            slotReg.pRedisServ = nullptr;
            slotReg.strHost = pSubReply->element[2]->element[0]->str;
            slotReg.nPort = pSubReply->element[2]->element[1]->integer;
            pvecSlot->push_back(slotReg);
        }
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
        return RC_REPLY_ERR;
    else
        return RC_TYPE_ERR;
}

// CRedisCommand methods
CRedisCommand::CRedisCommand(const std::string &strCmd, bool bShareMem)
    : m_strCmd(strCmd), m_bShareMem(bShareMem), m_nArgs(0), m_nIdx(0), m_pszArgs(nullptr),
      m_pnArgsLen(nullptr), m_pReply(nullptr), m_nSlot(-1), m_funcConv(FUNC_DEF_CONV)
{
}

void CRedisCommand::ClearArgs()
{
    if (m_pszArgs)
    {
        if (!m_bShareMem)
        {
            for (int i = 0; i < m_nArgs; ++i)
                delete [] m_pszArgs[i];
        }
        delete [] m_pszArgs;
    }
    if (m_pnArgsLen)
        delete [] m_pnArgsLen;
    if (m_pReply)
        freeReplyObject(m_pReply);
    m_pszArgs = nullptr;
    m_pnArgsLen = nullptr;
    m_pReply = nullptr;
    m_nArgs = 0;
    m_nIdx = 0;
}

std::string CRedisCommand::FetchErrMsg() const
{
    std::string strErrMsg;
    if (m_pReply && m_pReply->type == REDIS_REPLY_ERROR)
        strErrMsg.assign(m_pReply->str, m_pReply->len);
    return strErrMsg;
}

bool CRedisCommand::IsMovedErr() const
{
    return FetchErrMsg().substr(0, 5) == "MOVED";
}

void CRedisCommand::DumpArgs() const
{
    std::cout << "total " << m_nArgs << " args" << std::endl;
    for (int i = 0; i < m_nArgs; ++i)
        std::cout << i + 1 << " : " << m_pszArgs[i] << std::endl;
}

void CRedisCommand::DumpReply() const
{
    if (!m_pReply)
        std::cout << "no reply" << std::endl;
    else
        std::cout << m_pReply;
}

void CRedisCommand::SetArgs()
{
    InitMemory(1);
}

void CRedisCommand::SetArgs(const std::string &strArg)
{
    InitMemory(2);
    AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::vector<std::string> &vecArg)
{
    InitMemory(vecArg.size() + 1);
    for (auto &strArg : vecArg)
        AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::string &strArg2)
{
    InitMemory(3);
    AppendValue(strArg1);
    AppendValue(strArg2);
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::vector<std::string> &vecArg2)
{
    InitMemory(vecArg2.size() + 2);
    AppendValue(strArg1);
    for (auto &strArg : vecArg2)
        AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::vector<std::string> &vecArg1, const std::vector<std::string> &vecArg2)
{
    if (vecArg1.size() != vecArg2.size())
        return;

    InitMemory(vecArg1.size() * 2 + 1);
    for (size_t i = 0; i < vecArg1.size(); ++i)
    {
        AppendValue(vecArg1[i]);
        AppendValue(vecArg2[i]);
    }
}

void CRedisCommand::SetArgs(const std::map<std::string, std::string> &mapArg)
{
    InitMemory(mapArg.size() * 2 + 1);
    for (auto &kvPair : mapArg)
    {
        AppendValue(kvPair.first);
        AppendValue(kvPair.second);
    }
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::map<std::string, std::string> &mapArg2)
{
    InitMemory(mapArg2.size() * 2 + 2);
    AppendValue(strArg1);
    for (auto &kvPair : mapArg2)
    {
        AppendValue(kvPair.first);
        AppendValue(kvPair.second);
    }
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::string &strArg2, const std::string &strArg3)
{
    InitMemory(4);
    AppendValue(strArg1);
    AppendValue(strArg2);
    AppendValue(strArg3);
}

void CRedisCommand::InitMemory(int nArgs)
{
    ClearArgs();

    m_nArgs = nArgs;
    m_pszArgs = new char *[m_nArgs];
    m_pnArgsLen = new size_t[m_nArgs];
    AppendValue(m_strCmd);
}

void CRedisCommand::AppendValue(const std::string &strVal)
{
    if (m_nIdx >= m_nArgs)
        return;

    m_pnArgsLen[m_nIdx] = strVal.size();
    if (m_bShareMem)
        m_pszArgs[m_nIdx] = (char *)strVal.data();
    else
    {
        m_pszArgs[m_nIdx] = new char[strVal.size()];
        memcpy(m_pszArgs[m_nIdx], strVal.data(), strVal.size());
    }
    ++m_nIdx;
}

int CRedisCommand::CmdRequest(redisContext *pContext)
{
    if (m_nArgs <= 0)
        return RC_PARAM_ERR;

    if (!pContext)
        return RC_RQST_ERR;

    if (m_pReply)
    {
        freeReplyObject(m_pReply);
        m_pReply = nullptr;
    }

    m_pReply = static_cast<redisReply *>(redisCommandArgv(pContext, m_nArgs, (const char **)m_pszArgs, (const size_t *)m_pnArgsLen));
    return m_pReply ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::CmdAppend(redisContext *pContext)
{
    if (m_nArgs <= 0)
        return RC_PARAM_ERR;

    if (!pContext)
        return RC_RQST_ERR;

    int nRet = redisAppendCommandArgv(pContext, m_nArgs, (const char **)m_pszArgs, (const size_t *)m_pnArgsLen);
    return nRet == REDIS_OK ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::CmdReply(redisContext *pContext)
{
    if (m_pReply)
    {
        freeReplyObject(m_pReply);
        m_pReply = nullptr;
    }

    return redisGetReply(pContext, (void **)&m_pReply) == REDIS_OK ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::FetchResult(const TFuncFetch &funcFetch)
{
    return m_funcConv(funcFetch(m_pReply), m_pReply);
}

// CRedisServer methods
CRedisServer::CRedisServer(const std::string &strHost, int nPort, int nTimeout, int nConnNum)
    : m_strHost(strHost), m_nPort(nPort), m_nTimeout(nTimeout), m_nConnNum(nConnNum)
{
    struct timeval tmTimeout = {m_nTimeout, 0};
    redisContext *pContext = nullptr;
    for (int i = 0; i < m_nConnNum; ++i)
    {
        if ((pContext = redisConnectWithTimeout(m_strHost.c_str(), m_nPort, tmTimeout)))
            m_queIdleConn.push(pContext);
    }
}

CRedisServer::~CRedisServer()
{
    m_mutexConn.lock();
    while (!m_queIdleConn.empty())
    {
        redisContext *pContext = m_queIdleConn.front();
        redisFree(pContext);
        m_queIdleConn.pop();
    }
    m_mutexConn.unlock();
}

void CRedisServer::SetSlave(const std::string &strHost, int nPort)
{
    m_mapSlaveHost.insert(std::make_pair(strHost, nPort));
}

redisContext * CRedisServer::FetchConnection()
{
    redisContext *pContext = nullptr;
    m_mutexConn.lock();
    if (!m_queIdleConn.empty())
    {
        pContext = m_queIdleConn.front();
        m_queIdleConn.pop();
    }
    m_mutexConn.unlock();
    return pContext;
}

void CRedisServer::ReturnConnection(redisContext *pContext)
{
    m_mutexConn.lock();
    m_queIdleConn.push(pContext);
    m_mutexConn.unlock();
}

bool CRedisServer::TryConnectSlave()
{
    return true;
}

int CRedisServer::ServRequest(CRedisCommand *pRedisCmd)
{
    redisContext *pContext = nullptr;
    int nTry = RQST_RETRY_TIMES;
    while (nTry--)
    {
        if ((pContext = FetchConnection()))
            break;
        usleep(100);
    }

    if (!pContext)
        return RC_NO_RESOURCE;

    int nRet = pRedisCmd->CmdRequest(pContext);
    ReturnConnection(pContext);
    if (nRet == RC_RQST_ERR && !m_mapSlaveHost.empty())
        return TryConnectSlave() ? ServRequest(pRedisCmd) : nRet;
    else
        return nRet;
}

int CRedisServer::ServRequest(std::vector<CRedisCommand *> &vecRedisCmd)
{
    redisContext *pContext = nullptr;
    int nTry = RQST_RETRY_TIMES;
    while (nTry--)
    {
        if ((pContext = FetchConnection()))
            break;
        usleep(100);
    }

    if (!pContext)
        return RC_NO_RESOURCE;

    int nRet = RC_SUCCESS;
    for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
        nRet = vecRedisCmd[i]->CmdAppend(pContext);
    for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
        nRet = vecRedisCmd[i]->CmdReply(pContext);

    if (nRet == RC_RQST_ERR && !m_mapSlaveHost.empty())
        return TryConnectSlave() ? ServRequest(vecRedisCmd) : nRet;
    else
        return nRet;
}

// CRedisPipeline methods
CRedisPipeline::~CRedisPipeline()
{
    for (auto pRedisCmd : m_vecCmd)
        delete pRedisCmd;
}

void CRedisPipeline::QueueCommand(CRedisCommand *pRedisCmd)
{
    m_vecCmd.push_back(pRedisCmd);
}

int CRedisPipeline::FlushCommand(CRedisClient *pRedisCli)
{
    m_mapCmd.clear();
    for (auto &pRedisCmd : m_vecCmd)
    {
        CRedisServer *pRedisServ = pRedisCli->GetMatchedServer(pRedisCmd);
        auto it = m_mapCmd.find(pRedisServ);
        if (it != m_mapCmd.end())
            it->second.push_back(pRedisCmd);
        else
        {
            std::vector<CRedisCommand *> vecCmd;
            vecCmd.push_back(pRedisCmd);
            m_mapCmd.insert(std::make_pair(pRedisServ, vecCmd));
        }
    }

    int nRet = RC_SUCCESS;
    for (auto it = m_mapCmd.begin(); it != m_mapCmd.end() && nRet == RC_SUCCESS; ++it)
        nRet = it->first->ServRequest(it->second);
    return nRet;
}

int CRedisPipeline::FetchNext(TFuncFetch funcFetch)
{
    if (m_nCursor >= m_vecCmd.size())
        return RC_RESULT_EOF;
    else
        return m_vecCmd[m_nCursor++]->FetchResult(funcFetch);
}

int CRedisPipeline::FetchNext(redisReply **pReply)
{
    if (m_nCursor >= m_vecCmd.size())
        return RC_RESULT_EOF;
    else
    {
        *pReply = (redisReply *)m_vecCmd[m_nCursor++]->GetReply();
        return RC_SUCCESS;
    }
}

// CRedisClient methods
CRedisClient::CRedisClient(const std::string &strHost, int nPort, int nTimeout, int nConnNum)
{
    std::string::size_type nPos = strHost.find(':');
    m_strHost = (nPos == std::string::npos) ? strHost : strHost.substr(0, nPos);
    m_nPort = (nPos == std::string::npos) ? nPort : atoi(strHost.substr(nPos + 1).c_str());
    m_nTimeout = nTimeout <= 0 ? 100 : nTimeout;
    m_nConnNum = nConnNum <= 0 ? 5 : nConnNum;
    m_bCluster = false;
}

CRedisClient::~CRedisClient()
{
    CleanServer();
}

bool CRedisClient::Initialize()
{
    if (m_strHost.empty() || m_nPort <= 0)
        return false;

    CRedisServer *pRedisServ = new CRedisServer(m_strHost, m_nPort, m_nTimeout, m_nConnNum);
    if (!pRedisServ->IsValid())
        return false;

    std::string strInfo;
    std::map<std::string, std::string> mapInfo;
    CRedisCommand redisCmd("info");
    redisCmd.SetArgs();
    if (pRedisServ->ServRequest(&redisCmd) != RC_SUCCESS ||
        redisCmd.FetchResult(BIND_STR(&strInfo)) != RC_SUCCESS ||
        !ConvertToMapInfo(strInfo, mapInfo))
        return false;

    auto it = mapInfo.find("cluster_enabled");
    if (it == mapInfo.end())
        return false;
    else
        m_bCluster = (bool)atoi(it->second.c_str());

    m_vecRedisServ.push_back(pRedisServ);
    return m_bCluster ? LoadClusterSlots() : LoadSlaveInfo(mapInfo);
}

/* interfaces for generic */
int CRedisClient::Del(const std::string &strKey, Pipeline ppLine)
{
    return ExcuteImpl("del", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr), IntResConv());
}

int CRedisClient::Dump(const std::string &strKey, std::string *pstrVal, Pipeline ppLine)
{
    return ExcuteImpl("dump", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Exists(const std::string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExcuteImpl("exists", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Expire(const std::string &strKey, int nSec, Pipeline ppLine)
{
    return ExcuteImpl("expire", strKey, ConvertToString(nSec), HASH_SLOT(strKey), ppLine, BIND_INT(nullptr), IntResConv());
}

int CRedisClient::Keys(const std::string &strPattern, std::vector<std::string> *pvecVal)
{
    CRedisCommand redisCmd("keys");
    redisCmd.SetArgs(strPattern);

    int nRet;
    for (auto pRedisServ : m_vecRedisServ)
    {
        if ((nRet = pRedisServ->ServRequest(&redisCmd)) != RC_SUCCESS ||
            (nRet = redisCmd.FetchResult(BIND_VSTR(pvecVal))) != RC_SUCCESS)
        {
            if (pvecVal)
                pvecVal->clear();
            return nRet;
        }
    }
    return nRet;
}

int CRedisClient::Persist(const std::string &strKey, Pipeline ppLine)
{
    return ExcuteImpl("persist", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr), IntResConv(RC_NO_EFFECT));
}

int CRedisClient::Rename(const std::string &strKey, const std::string &strNewKey, Pipeline ppLine)
{
    return ExcuteImpl("rename", strKey, strNewKey, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr));
}

int CRedisClient::Renamenx(const std::string &strKey, const std::string &strNewKey, Pipeline ppLine)
{
    return ExcuteImpl("renamenx", strKey, strNewKey, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr));
}

int CRedisClient::Restore(const std::string &strKey, const std::string &strVal, int nTtl, Pipeline ppLine)
{
    return ExcuteImpl("restore", strKey, strVal, ConvertToString(nTtl), HASH_SLOT(strKey), ppLine, BIND_INT(nullptr));
}

int CRedisClient::Scan(long nCursor, const std::string &strPattern, long nCount, std::vector<std::string> &vecVal)
{
    return RC_SUCCESS;
}

int CRedisClient::Ttl(const std::string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExcuteImpl("ttl", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal), IntResConv(RC_KEY_NOT_EXIST, -2));
}

int CRedisClient::Type(const std::string &strKey, std::string *pstrVal, Pipeline ppLine)
{
    return ExcuteImpl("type", strKey, HASH_SLOT(strKey), ppLine, BIND_STU(pstrVal));
}

/* interfaces for string */
int CRedisClient::Append(const std::string &strKey, const std::string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExcuteImpl("append", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Decrby(const std::string &strKey, long nDecr, long *pnVal, Pipeline ppLine)
{
    return ExcuteImpl("decrby", strKey, ConvertToString(nDecr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Get(const std::string &strKey, std::string *pstrVal, Pipeline ppLine)
{
    return ExcuteImpl("get", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Getset(const std::string &strKey, std::string *pstrVal, Pipeline ppLine)
{
    return ExcuteImpl("getset", strKey, *pstrVal, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Incrby(const std::string &strKey, long nIncr, long *pnVal, Pipeline ppLine)
{
    return ExcuteImpl("incrby", strKey, ConvertToString(nIncr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Incrbyfloat(const std::string &strKey, double dIncr, double *pdVal, Pipeline ppLine)
{
    return RC_SUCCESS;
}

int CRedisClient::Mget(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal)
{
    if (vecKey.empty())
        return RC_SUCCESS;

    if (!m_bCluster)
        return ExcuteImpl("mget", vecKey, HASH_SLOT(vecKey[0]), nullptr, BIND_VSTR(pvecVal));
    else
    {
        Pipeline ppLine = CreatePipeline();
        if (!ppLine)
            return RC_RQST_ERR;

        int nRet = RC_SUCCESS;
        std::string strVal;
        for (auto &strKey : vecKey)
        {
            if ((nRet = Get(strKey, nullptr, ppLine)) != RC_SUCCESS)
                break;
        }
        FlushPipeline(ppLine);
        if (nRet == RC_SUCCESS)
        {
            while ((nRet = FetchReply(ppLine, &strVal)) != RC_RESULT_EOF)
            {
                if (nRet < 0)
                    break;
                else if (nRet == RC_KEY_NOT_EXIST)
                    pvecVal->push_back("");
                else
                    pvecVal->push_back(strVal);
            }
        }
        return nRet;
    }
}

int CRedisClient::Mset(const std::vector<std::string> &vecKey, const std::vector<std::string> &vecVal)
{
    return 0;
}

int CRedisClient::Set(const std::string &strKey, const std::string &strVal, Pipeline ppLine)
{
    return ExcuteImpl("set", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_STU(nullptr), StuResConv("OK"));
}

int CRedisClient::Strlen(const std::string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExcuteImpl("strlen", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

//int CRedisClient::Blpop(const std::string &strKey, int nTimeout, std::string *pstrVal)
//{
//    return ExcuteImpl("blpop", BIND_STR(pstrVal), strKey, HASH_SLOT(strKey));
//}
//
//int CRedisClient::Brpop(const std::string &strKey, int nTimeout, std::string *pstrVal)
//{
//    return ExcuteImpl("brpop", BIND_STR(pstrVal), strKey, HASH_SLOT(strKey));
//}
//
//int CRedisClient::Lindex(const std::string &strKey, long nIndex, std::string *pstrVal, Pipeline ppLine)
//{
//    return ExcuteImpl("lindex", BIND_STR(pstrVal), strKey, ConvertToString(nIndex), HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Llen(const std::string &strKey, long *pnVal, Pipeline ppLine)
//{
//    return ExcuteImpl("llen", BIND_INT(pnVal), strKey, HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Lpop(const std::string &strKey, std::string *pstrVal, Pipeline ppLine)
//{
//    return ExcuteImpl("lpop", BIND_STR(pstrVal), strKey, HASH_SLOT(strKey), ppLine);
//}
//
int CRedisClient::Lpush(const std::string &strKey, const std::string &strVal, Pipeline ppLine)
{
    return ExcuteImpl("lpush", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr));
}

//int CRedisClient::Lpush(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine)
//{
//    return ExcuteImpl("lpush", BIND_INT(nullptr), strKey, vecVal, HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Lpushx(const std::string &strKey, const std::string &strVal, Pipeline ppLine)
//{
//    return ExcuteImpl("lpushx", BIND_INT(nullptr), strKey, strVal, HASH_SLOT(strKey), ppLine);
//}

int CRedisClient::Lrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal, Pipeline ppLine)
{
    return ExcuteImpl("lrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

//int CRedisClient::Lrem(const std::string &strKey, long nCount, const std::string &strVal, Pipeline ppLine)
//{
//    return ExcuteImpl("lrem", BIND_INT(nullptr), strKey, ConvertToString(nCount), strVal, HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Lset(const std::string &strKey, long nIndex, const std::string &strVal, Pipeline ppLine)
//{
//    return ExcuteImpl("lset", BIND_INT(nullptr), strKey, ConvertToString(nIndex), strVal, HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Ltrim(const std::string &strKey, long nStart, long nStop, Pipeline ppLine)
//{
//    return ExcuteImpl("ltrim", BIND_INT(nullptr), strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Rpop(const std::string &strKey, std::string *pstrVal, Pipeline ppLine)
//{
//    return ExcuteImpl("rpop", BIND_STR(pstrVal), strKey, HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Rpush(const std::string &strKey, const std::string &strVal, Pipeline ppLine)
//{
//    return ExcuteImpl("rpush", BIND_STR(nullptr), strKey, strVal, HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Rpush(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine)
//{
//    return ExcuteImpl("rpush", BIND_INT(nullptr), strKey, vecVal, HASH_SLOT(strKey), ppLine);
//}
//
//int CRedisClient::Rpushx(const std::string &strKey, const std::string &strVal, Pipeline ppLine)
//{
//    return ExcuteImpl("rpushx", BIND_INT(nullptr), strKey, strVal, HASH_SLOT(strKey), ppLine);
//}

/* interfaces for set */
//int CRedisClient::Sadd(const std::string &strKey, const std::string &strVal, Pipeline = nullptr);
//int CRedisClient::Scard(const std::string &strKey, long *pnVal, Pipeline = nullptr);
//int CRedisClient::Sdiff(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Sinter(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Sismember(const std::string &strKey, const std::string &strVal, long *pnVal, Pipeline ppLine = nullptr);
//int CRedisClient::Smembers(const std::string &strKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Spop(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);
//int CRedisClient::Srandmember(const std::string &strKey, long nCount, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Srem(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
//int CRedisClient::Srem(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Sunion(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);

/* interfaces for hash */
//int CRedisClient::Hdel(const std::string &strKey, const std::string &strField, Pipeline ppLine = nullptr);
//int CRedisClient::Hexists(const std::string &strKey, const std::string &strField, long *pnVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hget(const std::string &strKey, const std::string &strField, std::string *pstrVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hgetall(const std::string &strKey, std::vector<std::string> *pvecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hincrby(const std::string &strKey, const std::string &strField, long nIncr, Pipeline ppLine = nullptr);
//int CRedisClient::Hincrbyfloat(const std::string &strKey, double dIncr, Pipeline ppLine = nullptr);
//int CRedisClient::Hkeys(const std::string &strKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hlen(const std::string &strKey, long *pnVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hmget(const std::string &strKey, const std::vector<std::string> &vecField, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hsget(const std::string &strKey, const std::vector<std::string> &vecField, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hset(const std::string &strKey, const std::string &strField, const std::string &strVal, Pipeline ppLine = nullptr);
//int CRedisClient::Hvals(const std::string &strKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);

/* interfaces for sorted set */
//int CRedisClient::Zadd(const std::string &strKey, double dScore, const std::string &strVal, Pipeline = nullptr);
//int CRedisClient::Zcard(const std::string &strKey, long *pnVal, Pipeline = nullptr);
//int CRedisClient::Zcount(const std::string &strKey, double dMin, double dMax, long *pnVal, Pipeline ppLine = nullptr);
//int CRedisClient::Zincrby(const std::string &strKey, double dIncr, const std::string &strVal, Pipeline ppLine = nullptr);
//int CRedisClient::Zrange(const std::string &strKey, long nStart, long nStop, const std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Zrem(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
//int CRedisClient::Zrem(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Zscore(const std::string &strKey, const std::string> &strKey, const std::string &strVal, Pipeline ppLine = nullptr);

Pipeline CRedisClient::CreatePipeline()
{
    return new CRedisPipeline();
}

int CRedisClient::FlushPipeline(Pipeline ppLine)
{
    if (!ppLine)
        return RC_PIPELINE_ERR;

    CRedisPipeline *pPipeline = static_cast<CRedisPipeline *>(ppLine);
    int nRet = pPipeline->FlushCommand(this);
    if (!m_bCluster || !TryReloadSlots())
        return nRet;
    else
        return pPipeline->FlushCommand(this);
}

#define MacroDefine(type, func) \
int CRedisClient::FetchReply(Pipeline ppLine, type tVal) \
{ \
    if (!ppLine) \
        return RC_NO_RESOURCE; \
    \
    CRedisPipeline *pPipeline = static_cast<CRedisPipeline *>(ppLine); \
    return pPipeline->FetchNext(func(tVal)); \
}

MacroDefine(long *, BIND_INT)
MacroDefine(std::string *, BIND_STR)
MacroDefine(std::vector<long> *, BIND_VINT)
MacroDefine(std::vector<std::string> *, BIND_VSTR)
#undef MacroDefine

int CRedisClient::FetchReply(Pipeline ppLine, redisReply **pReply)
{
    if (!ppLine)
        return RC_NO_RESOURCE;

    CRedisPipeline *pPipeline = static_cast<CRedisPipeline *>(ppLine);
    return pPipeline->FetchNext(pReply);
}

void CRedisClient::FreePipeline(Pipeline ppLine)
{
    if (!ppLine)
        return;

    CRedisPipeline *pPipeline = static_cast<CRedisPipeline *>(ppLine);
    delete pPipeline;
}

// private methods
bool CRedisClient::LoadSlaveInfo(const std::map<std::string, std::string> &mapInfo)
{
    auto it = mapInfo.find("connected_slaves");
    if (it == mapInfo.end())
        return true;

    std::stringstream ss;
    std::string strItem;
    int nSlave = atoi(it->second.c_str());
    for (int i = 0; i < nSlave; ++i)
    {
        ss.str();
        ss << "slave" << i;
        it = mapInfo.find(ss.str());
        if (it == mapInfo.end())
            continue;

        std::string strHost;
        int nPort = -1;
        ss.str(it->second);
        while (ss >> strItem)
        {
            if (strItem.substr(0, 3) == "ip=")
                strHost = strItem.substr(3);
            else if (strItem.substr(0, 5) == "port=")
                nPort = atoi(strItem.substr(5).c_str());
        }
        if (!strHost.empty() && nPort != -1)
            m_vecRedisServ[0]->SetSlave(strHost, nPort);
    }
    return true;
}

bool CRedisClient::LoadClusterSlots()
{
    std::vector<CRedisServer *> vecRedisServ;
    std::vector<SlotRegion> vecSlot;
    CRedisCommand redisCmd("cluster");
    redisCmd.SetArgs("slots");

    for (size_t i = 0; i < m_vecRedisServ.size(); ++i)
    {
        CRedisServer *pRedisServ = m_vecRedisServ[i];
        if (!pRedisServ->IsValid())
            return false;

        CRedisServer *pSlotServ = nullptr;
        if (pRedisServ->ServRequest(&redisCmd) == RC_SUCCESS &&
            redisCmd.FetchResult(BIND_SLOT(&vecSlot)) == RC_SUCCESS)
        {
            for (auto &slotReg : vecSlot)
            {
                if (!(pSlotServ = FindServer(vecRedisServ, slotReg.strHost, slotReg.nPort)))
                {
                    pSlotServ = new CRedisServer(slotReg.strHost, slotReg.nPort, m_nTimeout, m_nConnNum);
                    if (!pSlotServ->IsValid())
                        return false;
                    vecRedisServ.push_back(pSlotServ);
                }
                slotReg.pRedisServ = pSlotServ;
            }

            CleanServer();
            std::sort(vecSlot.begin(), vecSlot.end());
            m_vecRedisServ = vecRedisServ;
            m_vecSlot = vecSlot;
            return true;
        }
    }
    return false;
}

bool CRedisClient::TryReloadSlots()
{
    int nRetry = SLOTS_RELOAD_TIMES;
    while (!LoadClusterSlots() && nRetry-- > 0)
        usleep(100);
    return nRetry > 0;
}

void CRedisClient::CleanServer()
{
    m_vecSlot.clear();
    for (auto pRedisServ : m_vecRedisServ)
        delete pRedisServ;
    m_vecRedisServ.clear();
}

int CRedisClient::Excute(CRedisCommand *pRedisCmd, Pipeline ppLine)
{
    if (ppLine)
    {
        CRedisPipeline *pPipeline = static_cast<CRedisPipeline *>(ppLine);
        pPipeline->QueueCommand(pRedisCmd);
        return RC_SUCCESS;
    }

    if (!m_bCluster)
        return SimpleExcute(pRedisCmd);
    else
    {
        int nRet = SimpleExcute(pRedisCmd);
        if ((nRet = RC_SUCCESS && pRedisCmd->IsMovedErr()) || nRet == RC_RQST_ERR)
        {
            if (TryReloadSlots())
                return SimpleExcute(pRedisCmd);
        }
        return nRet;
    }
}

int CRedisClient::SimpleExcute(CRedisCommand *pRedisCmd)
{
    CRedisServer *pRedisServ = GetMatchedServer(pRedisCmd);
    return pRedisServ ? pRedisServ->ServRequest(pRedisCmd) : RC_RQST_ERR;
}

bool CRedisClient::ConvertToMapInfo(const std::string &strVal, std::map<std::string, std::string> &mapVal)
{
    std::stringstream ss(strVal);
    std::string strLine;
    while (std::getline(ss, strLine))
    {
        if (strLine.empty() || strLine[0] == '\r' || strLine[0] == '#')
            continue;

        std::string::size_type nPos = strLine.find(':');
        if (nPos == std::string::npos)
            return false;
        mapVal.insert(std::make_pair(strLine.substr(0, nPos), strLine.substr(nPos + 1)));
    }
    return true;
}

CRedisServer * CRedisClient::GetMatchedServer(const CRedisCommand *pRedisCmd) const
{
    if (!m_bCluster)
        return m_vecRedisServ.empty() ? nullptr : m_vecRedisServ[0];
    else if (pRedisCmd->GetSlot() == -1)
    {
        for (auto &pRedisServ : m_vecRedisServ)
        {
            if (pRedisServ->IsValid())
                return pRedisServ;
        }
        return nullptr;
    }
    else
    {
        auto pairIter = std::equal_range(m_vecSlot.begin(), m_vecSlot.end(), pRedisCmd->GetSlot(), CompSlot());
        if (pairIter.first != m_vecSlot.end() && pairIter.first != pairIter.second)
            return pairIter.first->pRedisServ;
        else
            return nullptr;
    }
}

CRedisServer * CRedisClient::FindServer(const std::vector<CRedisServer *> &vecRedisServ, const std::string &strHost, int nPort)
{
    for (auto &pRedisServ : vecRedisServ)
    {
        if (strHost == pRedisServ->GetHost() && nPort == pRedisServ->GetPort())
            return pRedisServ;
    }
    return nullptr;
}

