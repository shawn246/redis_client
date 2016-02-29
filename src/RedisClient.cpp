#include "RedisClient.hpp"

#define BIND_INT(val) std::bind(&FetchInteger, std::placeholders::_1, val)
#define BIND_STR(val) std::bind(&FetchString, std::placeholders::_1, val)
#define BIND_VINT(val) std::bind(&FetchIntegerArray, std::placeholders::_1, val)
#define BIND_VSTR(val) std::bind(&FetchStringArray, std::placeholders::_1, val)
#define BIND_MAP(val) std::bind(&FetchMap, std::placeholders::_1, val)
#define BIND_TIME(val) std::bind(&FetchTime, std::placeholders::_1, val)
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
        for (unsigned int j = 0; j < pReply->elements; ++j)
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

template <typename T>
std::string ConvertToString(T t)
{
    std::stringstream sstream;
    sstream << t;
    return sstream.str();
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
    IntResConv(int nConvRet = RC_OBJ_NOT_EXIST, long nVal = 0) : m_nConvRet(nConvRet), m_nVal(nVal) {}
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
    StuResConv() : m_strVal("OK") {}
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

class NilResConv
{
public:
    NilResConv();
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && pReply->type == REDIS_REPLY_NIL)
            return RC_OBJ_NOT_EXIST;
        return nRet;
    }
};

class ExistErrConv
{
public:
    ExistErrConv() : m_strErr("Target key name is busy") {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_REPLY_ERR && m_strErr == pReply->str)
            return RC_OBJ_EXIST;
        return nRet;
    }

private:
    std::string m_strErr;
};

static inline int FetchInteger(redisReply *pReply, long *pnVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        if (pnVal)
            *pnVal = pReply->integer;
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
        return RC_OBJ_NOT_EXIST;
    else
        return RC_REPLY_ERR;
}

static inline int FetchString(redisReply *pReply, std::string *pstrVal)
{
    if (pReply->type == REDIS_REPLY_STRING || pReply->type == REDIS_REPLY_STATUS)
    {
        if (pstrVal)
            pstrVal->assign(pReply->str, pReply->len);
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        if (pstrVal)
            pstrVal->clear();
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchIntegerArray(redisReply *pReply, std::vector<long> *pvecLongVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        int nRet = RC_SUCCESS;
        if (!pvecLongVal)
            return nRet;

        long nVal;
        pvecLongVal->clear();
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
    else
        return RC_REPLY_ERR;
}

static inline int FetchStringArray(redisReply *pReply, std::vector<std::string> *pvecStrVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        int nRet = RC_SUCCESS;
        if (!pvecStrVal)
            return nRet;

        std::string strVal;
        pvecStrVal->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            int nSubRet = FetchString(pReply->element[i], &strVal);
            if (nSubRet == RC_SUCCESS)
                pvecStrVal->push_back(strVal);
            else
                nRet = RC_PART_SUCCESS;
        }
        return nRet;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        if (pvecStrVal)
            pvecStrVal->clear();
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchMap(redisReply *pReply, std::map<std::string, std::string> *pmapFv)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        int nRet = RC_SUCCESS;
        if (!pmapFv)
            return nRet;

        if ((pReply->elements % 2) != 0)
            return RC_REPLY_ERR;

        std::string strFld;
        std::string strVal;
        pmapFv->clear();
        for (size_t i = 0; i < pReply->elements; )
        {
            int nSubRet = FetchString(pReply->element[i++], &strFld);
            if (nSubRet == RC_SUCCESS)
                nSubRet = FetchString(pReply->element[i++], &strVal);

            if (nSubRet == RC_SUCCESS)
                pmapFv->insert(std::make_pair(strFld, strVal));
            else
                return nSubRet;
        }
        return nRet;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchTime(redisReply *pReply, struct timeval *ptmVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (pReply->elements != 2 || pReply->element[0]->type != REDIS_REPLY_STRING ||
            pReply->element[1]->type != REDIS_REPLY_STRING)
            return RC_REPLY_ERR;

        if (ptmVal)
        {
            ptmVal->tv_sec = atol(pReply->element[0]->str);
            ptmVal->tv_usec = atol(pReply->element[1]->str);
        }
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchSlot(redisReply *pReply, std::vector<SlotRegion> *pvecSlot)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (!pvecSlot)
            return RC_SUCCESS;

        SlotRegion slotReg;
        pvecSlot->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            redisReply *pSubReply = pReply->element[i];
            if (pSubReply->type != REDIS_REPLY_ARRAY || pSubReply->elements < 3)
                return RC_REPLY_ERR;

            slotReg.nStartSlot = pSubReply->element[0]->integer;
            slotReg.nEndSlot = pSubReply->element[1]->integer;
            slotReg.pRedisServ = nullptr;
            slotReg.strHost = pSubReply->element[2]->element[0]->str;
            slotReg.nPort = pSubReply->element[2]->element[1]->integer;
            pvecSlot->push_back(slotReg);
        }
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

// CRedisCommand methods
bool CRedisCommand::CmdRequest(redisContext *pContext)
{
    if (m_nArgs <= 0)
        return SetErrInfo(RC_PARAM_ERR);

    if (!pContext)
        return SetErrInfo(RC_RQST_ERR, "No valid connection");

    m_pReply = static_cast<redisReply *>(redisCommandArgv(pContext, m_nArgs,
                                         (const char **)m_pszArgs, (const size_t *)m_pnArgsLen));
    return ParseReply();
}

bool CRedisCommand::CmdAppend(redisContext *pContext)
{
    if (m_nArgs <= 0)
        return SetErrInfo(RC_PARAM_ERR);

    if (!pContext)
        return SetErrInfo(RC_RQST_ERR, "No valid connection");

    int nRet = redisAppendCommandArgv(pContext, m_nArgs, (const char **)m_pszArgs, (const size_t *)m_pnArgsLen);
    return nRet == REDIS_OK ? SetErrInfo(RC_SUCCESS) : SetErrInfo(RC_RQST_ERR, pContext->errstr);
}

bool CRedisCommand::CmdReply(redisContext *pContext)
{
    if (!pContext)
        return SetErrInfo(RC_RQST_ERR, "No valid connection");

    if (m_pReply)
    {
        freeReplyObject(m_pReply);
        m_pReply = nullptr;
    }

    int nRet = redisGetReply(pContext, (void **)&m_pReply);
    return nRet == REDIS_OK ? ParseReply() ? SetErrInfo(RC_RQST_ERR, pContext->errstr);
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


// CRedisConnection methods
CRedisConnection::CRedisConnection(CRedisServer *pRedisServ) : m_pContext(nullptr), m_nUseTime(0), m_pRedisServ(pRedisServ)
{
    Reconnect();
}

int CRedisConnection::ConnRequest(CRedisCommand *pRedisCmd)
{
    time_t tmNow = time(nullptr);
    if (!m_pContext || tmNow - m_nUseTime >= m_pRedisServ->m_nSerTimeout)
    {
        if (!Reconnect())
            return RC_RQST_ERR;
    }

    int nRet = pRedisCmd->CmdRequest(m_pContext);
    if (nRet == RC_RQST_ERR)
    {
        if (tmNow - m_nUseTime < m_pRedisServ->m_nSerTimeout)
            return nRet;
        else if (!Reconnect())
            return RC_RQST_ERR;
        else
            nRet = pRedisCmd->CmdRequest(m_pContext);
    }

    if (nRet != RC_RQST_ERR)
        m_nUseTime = tmNow;
    return nRet;
}

int CRedisConnection::ConnRequest(std::vector<CRedisCommand *> &vecRedisCmd)
{
    time_t tmNow = time(nullptr);
    if (!m_pContext || tmNow - m_nUseTime >= m_pRedisServ->m_nSerTimeout)
    {
        if (!Reconnect())
            return RC_RQST_ERR;
    }

    int nRet = RC_SUCCESS;
    for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
        nRet = vecRedisCmd[i]->CmdAppend(m_pContext);
    for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
        nRet = vecRedisCmd[i]->CmdReply(m_pContext);
    return nRet;
}

bool CRedisConnection::ConnectToRedis(const std::string &strHost, int nPort, int nTimeout)
{
    if (m_pContext)
        redisFree(m_pContext);

    struct timeval tmTimeout = {nTimeout, 0};
    m_pContext = redisConnectWithTimeout(strHost.c_str(), nPort, tmTimeout);
    if (!m_pContext || m_pContext->err)
    {
        if (m_pContext)
        {
            redisFree(m_pContext);
            m_pContext = nullptr;
        }
        return false;
    }

    redisSetTimeout(m_pContext, tmTimeout);
    m_nUseTime = time(nullptr);
    return true;
}

bool CRedisConnection::Reconnect()
{
    if (!m_pRedisServ->m_strHost.empty() &&
        ConnectToRedis(m_pRedisServ->m_strHost, m_pRedisServ->m_nPort, m_pRedisServ->m_nCliTimeout))
        return true;

    for (auto &hostPair : m_pRedisServ->m_vecHosts)
    {
        if (ConnectToRedis(hostPair.first, hostPair.second, m_pRedisServ->m_nCliTimeout))
        {
            m_pRedisServ->m_strHost = hostPair.first;
            m_pRedisServ->m_nPort = hostPair.second;
            return true;
        }
    }

    m_pRedisServ->m_strHost.clear();
    return false;
}

// CRedisServer methods
CRedisServer::CRedisServer(const std::string &strHost, int nPort, int nTimeout, int nConnNum)
    : m_strHost(strHost), m_nPort(nPort), m_nCliTimeout(nTimeout), m_nSerTimeout(0), m_nConnNum(nConnNum)
{
    SetSlave(strHost, nPort);
    Initialize();
}

CRedisServer::~CRedisServer()
{
    CleanConn();
}

void CRedisServer::CleanConn()
{
    m_mutexConn.lock();
    while (!m_queIdleConn.empty())
    {
        delete m_queIdleConn.front();
        m_queIdleConn.pop();
    }
    m_mutexConn.unlock();
}

void CRedisServer::SetSlave(const std::string &strHost, int nPort)
{
    m_vecHosts.push_back(std::make_pair(strHost, nPort));
}

CRedisConnection * CRedisServer::FetchConnection()
{
    CRedisConnection *pRedisConn = nullptr;
    m_mutexConn.lock();
    if (!m_queIdleConn.empty())
    {
        pRedisConn = m_queIdleConn.front();
        m_queIdleConn.pop();
    }
    m_mutexConn.unlock();
    return pRedisConn;
}

void CRedisServer::ReturnConnection(CRedisConnection *pRedisConn)
{
    m_mutexConn.lock();
    m_queIdleConn.push(pRedisConn);
    m_mutexConn.unlock();
}

bool CRedisServer::Initialize()
{
    CleanConn();

    for (int i = 0; i < m_nConnNum; ++i)
    {
        CRedisConnection *pRedisConn = new CRedisConnection(this);
        if (pRedisConn->IsValid())
            m_queIdleConn.push(pRedisConn);
    }

    if (!m_queIdleConn.empty())
    {
        std::vector<std::string> vecTimeout;
        CRedisCommand redisCmd("config");
        redisCmd.SetArgs("get", "timeout");
        if (ServRequest(&redisCmd) == RC_SUCCESS &&
            redisCmd.FetchResult(BIND_VSTR(&vecTimeout)) == RC_SUCCESS &&
            vecTimeout.size() == 2)
            m_nSerTimeout = atoi(vecTimeout[1].c_str());
    }
    return !m_queIdleConn.empty();
}

int CRedisServer::ServRequest(CRedisCommand *pRedisCmd)
{
    CRedisConnection *pRedisConn = nullptr;
    int nTry = RQST_RETRY_TIMES;
    while (nTry--)
    {
        if ((pRedisConn = FetchConnection()))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!pRedisConn)
        return RC_NO_RESOURCE;

    int nRet = pRedisConn->ConnRequest(pRedisCmd);
    ReturnConnection(pRedisConn);
    return nRet;
}

int CRedisServer::ServRequest(std::vector<CRedisCommand *> &vecRedisCmd)
{
    CRedisConnection *pRedisConn = nullptr;
    int nTry = RQST_RETRY_TIMES;
    while (nTry--)
    {
        if ((pRedisConn = FetchConnection()))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!pRedisConn)
        return RC_NO_RESOURCE;

    int nRet = pRedisConn->ConnRequest(vecRedisCmd);
    ReturnConnection(pRedisConn);
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
CRedisClient::CRedisClient()
    : m_nPort(-1), m_nTimeout(-1), m_nConnNum(-1), m_bCluster(false),
      m_bValid(true), m_bExit(false), m_pThread(nullptr)
{
#if defined(linux) || defined(__linux) || defined(__linux__)
    pthread_rwlockattr_init(&m_rwAttr);
    pthread_rwlockattr_setkind_np(&m_rwAttr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&m_rwLock, &m_rwAttr);
#else
    pthread_rwlock_init(&m_rwLock, nullptr);
#endif
}

CRedisClient::~CRedisClient()
{
    m_bValid = false;
    m_bExit = true;
    {
        CSafeLock safeLock(&m_rwLock);
        safeLock.WriteLock();
        m_condAny.notify_all();
    }
    if (m_pThread)
    {
        m_pThread->join();
        delete m_pThread;
    }

    CleanServer();
#if defined(linux) || defined(__linux) || defined(__linux__)
    pthread_rwlockattr_destroy(&m_rwAttr);
#endif
    pthread_rwlock_destroy(&m_rwLock);
}

bool CRedisClient::Initialize(const std::string &strHost, int nPort, int nTimeout, int nConnNum)
{
    std::string::size_type nPos = strHost.find(':');
    m_strHost = (nPos == std::string::npos) ? strHost : strHost.substr(0, nPos);
    m_nPort = (nPos == std::string::npos) ? nPort : atoi(strHost.substr(nPos + 1).c_str());
    m_nTimeout = nTimeout;
    m_nConnNum = nConnNum;
    if (m_strHost.empty() || m_nPort <= 0 || m_nTimeout <= 0 || m_nConnNum <= 0)
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
    m_bValid = (m_bCluster ? LoadClusterSlots() : LoadSlaveInfo(mapInfo)) &&
        (m_pThread = new std::thread(std::bind(&CRedisClient::operator(), this))) != nullptr;
    return m_bValid;
}

bool CRedisClient::Request(CRedisCommand *pRedisCmd)
{
    if (!m_bValid)
        return false;

    CSafeLock safeLock(&m_rwLock);
    if (!safeLock.ReadLock() || !m_bValid)
        return false;

    CRedisServer *pRedisServ = MatchServer(pRedisCmd->GetSlot());
    if (!pRedisServ)
        return false;

    if (pRedisServ->ServRequest(pRedisCmd))
        return true;
    else
    {
        if ((pRedisCmd->GetErrCode() == RC_RQST_ERR ||
            pRedisCmd->GetErrCode() == RC_MOVED_ERR) &&
            WaitForRefresh())
            return pRedisServ->ServRequest(pRedisCmd);
        else
            return false;
    }
}

bool CRedisClient::Request(CRedisPipeline *pRedisPipe)
{
    int nRetryTime = 0;
    bool bRet = false;
    while (1)
    {
        CRedisServer *pRedisServ = nullptr;
        std::map<CRedisServer *, CRedisCommandAry> mapCmd;
        for (auto &pRedisCmd : pRedisPipe->m_vecCmd)
        {
            if (!(pRedisServ = MatchServer(pRedisCmd->GetSlot())))
                return false;

            auto it = mapCmd.find(pRedisServ);
            if (it != mapCmd.end())
                it->second.Append(pRedisCmd);
            else
            {
                CRedisCommandAry redisCmdAry;
                redisCmdAry.Append(pRedisCmd);
                mapCmd.insert(std::make_pair(pRedisServ, redisCmdAry));
            }
        }

        bool bRetry = false;
        for (auto it = mapCmd.begin(); it != mapCmd.end() && bRet; ++it)
        {
            bRet = it->first->ServRequest(it->second);
            if (!bRet)
            {
                pRedisPipe->m_nErrCode = it->second.m_nErrCode;
                pRedisPipe->m_strErrMsg = it->second.m_strErrMsg;

                if ((it->second->GetErrCode() == RC_RQST_ERR ||
                    it->second->GetErrCode() == RC_MOVED_ERR) &&
                    nRetryTime++ <= 1 &&
                    WaitForRefresh())
                {
                    bRetry = true;
                    break;
                }
            }
        }

        if (!bRetry)
            return bRet;
    }
}

void CRedisClient::operator()()
{
    while (!m_bExit)
    {
        {
            CSafeLock safeLock(&m_rwLock);
            safeLock.WriteLock();
            if (m_bValid)
                m_condAny.wait(safeLock);

            m_bValid = m_bCluster ? LoadClusterSlots() : m_vecRedisServ[0]->Initialize();
        }

        if (!m_bValid)
            std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

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
        {
            for (auto pRedisServ : vecRedisServ)
                delete pRedisServ;
            return false;
        }

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
                    {
                        for (auto pRedisServ : vecRedisServ)
                            delete pRedisServ;
                        return false;
                    }
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

bool CRedisClient::WaitForRefresh()
{
    {
        CSafeLock safeLock(&m_rwLock);
        if (safeLock.TryReadLock())
            m_condAny.notify_all();
    }

    int nRetry = WAIT_RETRY_TIMES;
    while (!m_bValid && nRetry-- > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return m_bValid;
}

void CRedisClient::CleanServer()
{
    m_vecSlot.clear();
    for (auto pRedisServ : m_vecRedisServ)
        delete pRedisServ;
    m_vecRedisServ.clear();
}

CRedisServer * CRedisClient::MatchServer(int nSlot) const
{
    if (m_bCluster && nSlot != -1)
        return FindServer(pRedisCmd->GetSlot());
    else
    {
        for (auto &pRedisServ : m_vecRedisServ)
        {
            if (pRedisServ->IsValid())
                return pRedisServ;
        }
        return nullptr;
    }
}

CRedisServer * CRedisClient::FindServer(int nSlot) const
{
    auto pairIter = std::equal_range(m_vecSlot.begin(), m_vecSlot.end(), nSlot, CompSlot());
    if (pairIter.first != m_vecSlot.end() && pairIter.first != pairIter.second)
        return pairIter.first->pRedisServ;
    else
        return nullptr;
}

bool CRedisClient::OnSameServer(const std::string &strKey1, const std::string &strKey2) const
{
    return m_bCluster ? FindServer(HASH_SLOT(strKey1)) == FindServer(HASH_SLOT(strKey2)) : true;
}

bool CRedisClient::OnSameServer(const std::vector<std::string> &vecKey) const
{
    if (m_bCluster || vecKey.size() <= 1)
        return true;

    for (int i = 0; i < vecKey.size() - 2; ++i)
    {
        if (!OnSameServer(vecKey[i], vecKey[i + 1]))
            return false;
    }
    return true;
}

bool CRedisClient::ConvertInfoToMap(const std::string &strVal, std::map<std::string, std::string> &mapVal)
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

CRedisServer * CRedisClient::FindServer(const std::vector<CRedisServer *> &vecRedisServ, const std::string &strHost, int nPort) const
{
    for (auto &pRedisServ : vecRedisServ)
    {
        if (strHost == pRedisServ->GetHost() && nPort == pRedisServ->GetPort())
            return pRedisServ;
    }
    return nullptr;
}

//////////// new ///////////
bool CRedisClient::Execute(CRedisCommand *pRedisCmd)
{
    if (!m_bValid)
        return false;

    CSafeLock safeLock(&m_rwLock);
    if (!safeLock.ReadLock() || !m_bValid)
        return false;

    CRedisServer *pRedisServ = MatchServer(pRedisCmd->GetSlot());
    if (!pRedisServ)
        return false;

    if (pRedisServ->ServRequest(pRedisCmd))
        return true;
    else
    {
        if ((pRedisCmd->GetErrCode() == RC_RQST_ERR ||
            pRedisCmd->GetErrCode() == RC_MOVED_ERR) &&
            WaitForRefresh())
            return pRedisServ->ServRequest(pRedisCmd);
        else
            return false;
    }
}

bool CRedisClient::Execute(CRedisPipeline *pRedisPipe)
{

}
