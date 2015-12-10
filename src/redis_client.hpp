#include "redis/hiredis.h"
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <sstream>
#include <functional>

#define RC_RESULT_EOF       4
#define RC_NO_EFFECT        3
#define RC_KEY_NOT_EXIST    2
#define RC_PART_SUCCESS     1
#define RC_SUCCESS          0
#define RC_PARAM_ERR        -1
#define RC_RQST_ERR         -2
#define RC_REPLY_ERR        -3
#define RC_TYPE_ERR         -4
#define RC_NO_RESOURCE      -5
#define RC_PIPELINE_ERR     -6
#define RC_SLOT_CHANGED     -100

#define RQST_RETRY_TIMES    2
#define SLOTS_RELOAD_TIMES  16

#define FUNC_DEF_CONV       [](int nRet, redisReply *) { return nRet; }

typedef enum
{
    CMD_NORMAL,
    CMD_COVER,
    CMD_OCCUPY
} CmdType;

typedef std::function<int (redisReply *)> TFuncFetch;
typedef std::function<int (int, redisReply *)> TFuncConvert;

template <typename T>
std::string ConvertToString(T t)
{
    std::stringstream sstream;
    sstream << t;
    return sstream.str();
}

class CRedisServer;
struct SlotRegion
{
    int nStartSlot;
    int nEndSlot;
    std::string strHost;
    int nPort;
    CRedisServer *pRedisServ;
};

class CRedisCommand
{
public:
    CRedisCommand(const std::string &strCmd, bool bShareMem = true);
    virtual ~CRedisCommand() { ClearArgs(); }
    void ClearArgs();
    void DumpArgs() const;
    void DumpReply() const;

    int GetSlot() const { return m_nSlot; }
    const redisReply * GetReply() const { return m_pReply; }
    std::string FetchErrMsg() const;
    bool IsMovedErr() const;

    void SetSlot(int nSlot) { m_nSlot = nSlot; }
    void SetConvFunc(TFuncConvert funcConv) { m_funcConv = funcConv; }

    void SetArgs();
    void SetArgs(const std::string &strArg);
    void SetArgs(const std::vector<std::string> &vecArg);
    void SetArgs(const std::string &strArg1, const std::string &strArg2);
    void SetArgs(const std::string &strArg1, const std::vector<std::string> &vecArg2);
    void SetArgs(const std::vector<std::string> &vecArg1, const std::vector<std::string> &vecArg2);
    void SetArgs(const std::map<std::string, std::string> &mapArg);
    void SetArgs(const std::string &strArg1, const std::vector<std::string> &vecArg2, const std::vector<std::string> &vecArg3);
    void SetArgs(const std::string &strArg1, const std::map<std::string, std::string> &mapArg2);
    void SetArgs(const std::string &strArg1, const std::string &strArg2, const std::string &strArg3);

    int CmdRequest(redisContext *pContext);
    int CmdAppend(redisContext *pContext);
    int CmdReply(redisContext *pContext);
    int FetchResult(const TFuncFetch &funcFetch);

private:
    void InitMemory(int nArgs);
    void AppendValue(const std::string &strVal);

protected:
    std::string m_strCmd;
    bool m_bShareMem;

    int m_nArgs;
    int m_nIdx;
    char **m_pszArgs;
    size_t *m_pnArgsLen;
    redisReply *m_pReply;

    int m_nSlot;
    TFuncConvert m_funcConv;
};

class CRedisServer
{
public:
    CRedisServer(const std::string &strHost, int nPort, int nTimeout, int nConnNum);
    virtual ~CRedisServer();

    void SetSlave(const std::string &strHost, int nPort);

    std::string GetHost() const { return m_strHost; }
    int GetPort() const { return m_nPort; }
    bool IsValid() const { return m_queIdleConn.size(); }

    // for the blocking request
    int ServRequest(CRedisCommand *pRedisCmd);

    // for the pipeline requirement
    int ServRequest(std::vector<CRedisCommand *> &vecRedisCmd);

private:
    redisContext *FetchConnection();
    void ReturnConnection(redisContext *pContext);
    bool TryConnectSlave();

private:
    std::string m_strHost;
    int m_nPort;
    int m_nTimeout;
    int m_nConnNum;

    std::queue<redisContext *> m_queIdleConn;
    std::map<std::string, int> m_mapSlaveHost;
    std::mutex m_mutexConn;
};

class CRedisClient;
class CRedisPipeline
{
    friend class CRedisClient;
public:
    ~CRedisPipeline();

private:
    CRedisPipeline() : m_nCursor(0) {}

    void QueueCommand(CRedisCommand *pRedisCmd);
    int FlushCommand(CRedisClient *pRedisCli);
    int FetchNext(TFuncFetch funcFetch);
    int FetchNext(redisReply **pReply);

private:
    std::map<CRedisServer *, std::vector<CRedisCommand *> > m_mapCmd;
    std::vector<CRedisCommand *> m_vecCmd;
    size_t m_nCursor;
};

typedef void * Pipeline;
class CRedisClient
{
    friend class CRedisPipeline;
public:
    CRedisClient(const std::string &strHost, int nPort, int nTimeout, int nConnNum);
    ~CRedisClient();
    bool Initialize();

    Pipeline CreatePipeline();
    int FlushPipeline(Pipeline ppLine);
    int FetchReply(Pipeline ppLine, long *pnVal);
    int FetchReply(Pipeline ppLine, std::string *pstrVal);
    int FetchReply(Pipeline ppLine, std::vector<long> *pvecLongVal);
    int FetchReply(Pipeline ppLine, std::vector<std::string> *pvecStrVal);
    int FetchReply(Pipeline ppLine, redisReply **pReply);
    void FreePipeline(Pipeline ppLine);

    /* interfaces for generic */
    int Del(const std::string &strKey, Pipeline ppLine = nullptr);
    int Dump(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Exists(const std::string &strKey, long *pnVal, Pipeline ppLine = nullptr);
    int Expire(const std::string &strKey, int nSec, Pipeline ppLine = nullptr);
    int Keys(const std::string &strPattern, std::vector<std::string> *pvecVal);
    int Persist(const std::string &strKey, Pipeline ppLine = nullptr);
    int Rename(const std::string &strKey, const std::string &strNewKey, Pipeline ppLine = nullptr);
    int Renamenx(const std::string &strKey, const std::string &strNewKey, Pipeline ppLine = nullptr);
    int Restore(const std::string &strKey, const std::string &strVal, int nTtl = 0, Pipeline ppLine = nullptr);
    int Scan(long nCursor, const std::string &strPattern, long nCount, std::vector<std::string> &vecVal);
    int Ttl(const std::string &strKey, long *pnVal, Pipeline ppLine = nullptr);
    int Type(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);

    /* interfaces for string */
    int Append(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr, Pipeline ppLine = nullptr);
    int Decrby(const std::string &strKey, long nDecr, long *pnVal = nullptr, Pipeline ppLine = nullptr);
    int Get(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Getset(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Incrby(const std::string &strKey, long nIncr, long *pnVal, Pipeline ppLine = nullptr);
    int Incrbyfloat(const std::string &strKey, double dIncr, double *pdVal, Pipeline ppLine = nullptr);
    int Mget(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal);
    int Mset(const std::vector<std::string> &vecKey, const std::vector<std::string> &vecVal);
    int Set(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
    int Strlen(const std::string &strKey, long *pnVal, Pipeline ppLine = nullptr);

    /* interfaces for list */
    int Blpop(const std::string &strKey, int nTimeout, std::string *pstrVal);
    int Brpop(const std::string &strKey, int nTimeout, std::string *pstrVal);
    int Lindex(const std::string &strKey, long nIndex, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Llen(const std::string &strKey, long *pnVal, Pipeline ppLine = nullptr);
    int Lpop(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Lpush(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
    int Lpush(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
    int Lpushx(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
    int Lrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Lrem(const std::string &strKey, long nCount, const std::string &strVal, Pipeline ppLine = nullptr);
    int Lset(const std::string &strKey, long nIndex, const std::string &strVal, Pipeline ppLine = nullptr);
    int Ltrim(const std::string &strKey, long nStart, long nStop, Pipeline ppLine = nullptr);
    int Rpop(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Rpush(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
    int Rpush(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
    int Rpushx(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);

    /* interfaces for set */
    int Sadd(const std::string &strKey, const std::string &strVal, Pipeline = nullptr);
    int Scard(const std::string &strKey, long *pnVal, Pipeline = nullptr);
    int Sdiff(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Sinter(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Sismember(const std::string &strKey, const std::string &strVal, long *pnVal, Pipeline ppLine = nullptr);
    int Smembers(const std::string &strKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Spop(const std::string &strKey, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Srandmember(const std::string &strKey, long nCount, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Srem(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
    int Srem(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
    int Sunion(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);

    /* interfaces for hash */
    int Hdel(const std::string &strKey, const std::string &strField, Pipeline ppLine = nullptr);
    int Hexists(const std::string &strKey, const std::string &strField, long *pnVal, Pipeline ppLine = nullptr);
    int Hget(const std::string &strKey, const std::string &strField, std::string *pstrVal, Pipeline ppLine = nullptr);
    int Hgetall(const std::string &strKey, std::vector<std::string> *pvecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Hincrby(const std::string &strKey, const std::string &strField, long nIncr, Pipeline ppLine = nullptr);
    int Hincrbyfloat(const std::string &strKey, double dIncr, Pipeline ppLine = nullptr);
    int Hkeys(const std::string &strKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Hlen(const std::string &strKey, long *pnVal, Pipeline ppLine = nullptr);
    int Hmget(const std::string &strKey, const std::vector<std::string> &vecField, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Hsget(const std::string &strKey, const std::vector<std::string> &vecField, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
    int Hset(const std::string &strKey, const std::string &strField, const std::string &strVal, Pipeline ppLine = nullptr);
    int Hvals(const std::string &strKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);

    /* interfaces for sorted set */
    int Zadd(const std::string &strKey, double dScore, const std::string &strVal, Pipeline = nullptr);
    int Zcard(const std::string &strKey, long *pnVal, Pipeline = nullptr);
    int Zcount(const std::string &strKey, double dMin, double dMax, long *pnVal, Pipeline ppLine = nullptr);
    int Zincrby(const std::string &strKey, double dIncr, const std::string &strVal, Pipeline ppLine = nullptr);
    int Zrange(const std::string &strKey, long nStart, long nStop, const std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
    int Zrem(const std::string &strKey, const std::string &strVal, Pipeline ppLine = nullptr);
    int Zrem(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine = nullptr);
    int Zscore(const std::string &strKey, const std::string &strVal, double *pdVal, Pipeline ppLine = nullptr);

private:
    static bool ConvertToMapInfo(const std::string &strVal, std::map<std::string, std::string> &mapVal);
    static bool GetValue(redisReply *pReply, std::string &strVal);
    static bool GetArray(redisReply *pReply, std::vector<std::string> &vecVal);
    static CRedisServer * FindServer(const std::vector<CRedisServer *> &vecRedisServ, const std::string &strHost, int nPort);

    void CleanServer();
    CRedisServer * GetMatchedServer(const CRedisCommand *pRedisCmd) const;
    bool LoadSlaveInfo(const std::map<std::string, std::string> &mapInfo);
    bool LoadClusterSlots();
    bool TryReloadSlots();
    int Excute(CRedisCommand *pRedisCmd, Pipeline ppLine = nullptr);
    int SimpleExcute(CRedisCommand *pRedisCmd);

    template <typename P>
    int ExcuteImpl(const std::string &strCmd, const P &tArg, int nSlot, Pipeline ppLine,
                   TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV)
    {
        CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
        pRedisCmd->SetArgs(tArg);
        pRedisCmd->SetSlot(nSlot);
        pRedisCmd->SetConvFunc(funcConv);
        int nRet = Excute(pRedisCmd, ppLine);
        if (nRet == RC_SUCCESS && !ppLine)
            nRet = pRedisCmd->FetchResult(funcFetch);
        if (!ppLine)
            delete pRedisCmd;
        return nRet;
    }

    template <typename P1, typename P2>
    int ExcuteImpl(const std::string &strCmd, const P1 &tArg1, const P2 &tArg2, int nSlot, Pipeline ppLine,
                   TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV)
    {
        CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
        pRedisCmd->SetArgs(tArg1, tArg2);
        pRedisCmd->SetSlot(nSlot);
        pRedisCmd->SetConvFunc(funcConv);
        int nRet = Excute(pRedisCmd, ppLine);
        if (nRet == RC_SUCCESS && !ppLine)
            nRet = pRedisCmd->FetchResult(funcFetch);
        if (!ppLine)
            delete pRedisCmd;
        return nRet;
    }

    template <typename P1, typename P2, typename P3>
    int ExcuteImpl(const std::string &strCmd, const P1 &tArg1, const P2 &tArg2, const P3 &tArg3, int nSlot, Pipeline ppLine,
                   TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV)
    {
        CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
        pRedisCmd->SetArgs(tArg1, tArg2, tArg3);
        pRedisCmd->SetSlot(nSlot);
        pRedisCmd->SetConvFunc(funcConv);
        int nRet = Excute(pRedisCmd, ppLine);
        if (nRet == RC_SUCCESS && !ppLine)
            nRet = pRedisCmd->FetchResult(funcFetch);
        if (!ppLine)
            delete pRedisCmd;
        return nRet;
    }

private:
    std::string m_strHost;
    int m_nPort;
    int m_nTimeout;
    int m_nConnNum;
    bool m_bCluster;
    std::vector<SlotRegion> m_vecSlot;
    std::vector<CRedisServer *> m_vecRedisServ;
};

