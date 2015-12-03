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
#define RC_SLOT_CHANGED     -100

#define RQST_RETRY_TIMES    2
#define INIT_RETRY_TIMES    16

#define FUNC_DEF_CONV       [](int nRet, redisReply *) { return nRet; }

typedef enum
{
    CMD_SYSTEM,
    CMD_MESSAGE,
    CMD_NORMAL
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
    CRedisCommand(const std::string &strCmd, CmdType eType = CMD_NORMAL, bool bShareMem = true);
    virtual ~CRedisCommand() { ClearArgs(); }
    void ClearArgs();
    void DumpArgs() const;
    void DumpReply() const;

    CmdType GetCmdType() const { return m_eType; }
    int GetSlot() const { return m_nSlot; }
    const redisReply * GetReply() const { return m_pReply; }
    std::string FetchErrMsg() const;

    void SetSlot(int nSlot) { m_nSlot = nSlot; }
    void SetFetchFunc(TFuncFetch funcFetch) { m_funcFetch = funcFetch; }
    void SetConvFunc(TFuncConvert funcConv) { m_funcConv = funcConv; }

    void SetArgs();
    void SetArgs(const std::string &strKey);
    void SetArgs(const std::vector<std::string> &vecKey);
    void SetArgs(const std::string &strKey, const std::string &strVal);
    void SetArgs(const std::vector<std::string> &vecKey, const std::vector<std::string> &vecVal);
    void SetArgs(const std::map<std::string, std::string> &maKeyVal);
    void SetArgs(const std::string &strKey, const std::vector<std::string> &vecKey, const std::vector<std::string> &vecVal);
    void SetArgs(const std::string &strKey, const std::map<std::string, std::string> &mapFieldVal);

    int CmdRequest(redisContext *pContext);
    int CmdAppend(redisContext *pContext);
    int CmdReply(redisContext *pContext);
    int FetchResult();
    int FetchResult(const TFuncFetch &funcFetch);

private:
    void InitMemory(int nArgs);
    void AppendValue(const std::string &strVal);

protected:
    std::string m_strCmd;
    CmdType m_eType;
    bool m_bShareMem;

    int m_nArgs;
    int m_nIdx;
    char **m_pszArgs;
    size_t *m_pnArgsLen;
    redisReply *m_pReply;

    int m_nSlot;
    TFuncFetch m_funcFetch;
    TFuncConvert m_funcConv;
};

class CRedisServer
{
public:
    CRedisServer(const std::string &strHost, int nPort, int nTimeout, int nConnNum);
    virtual ~CRedisServer();

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

private:
    std::string m_strHost;
    int m_nPort;
    int m_nTimeout;
    int m_nConnNum;

    std::queue<redisContext *> m_queIdleConn;
    std::mutex m_mutexConn;
};

class CRedisPipeline
{
    friend class CRedisClient;
public:
    ~CRedisPipeline();

private:
    CRedisPipeline() : m_nCursor(0) {}

    void QueueCommand(CRedisServer *pRedisServ, CRedisCommand *pRedisCmd);
    void FlushCommand();
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
public:
    CRedisClient(const std::string &strHost, int nPort, int nTimeout, int nConnNum);
    ~CRedisClient();
    bool Initialize();

    Pipeline CreatePipeline();
    void FlushPipeline(Pipeline ppLine);
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
    int Keys(const std::string &strPattern, std::vector<std::string> &vecVal);
    int Persist(const std::string &strKey, Pipeline ppLine = nullptr);
    int Rename(const std::string &strKey, const std::string &strNewKey, int nOpt, Pipeline ppLine = nullptr);
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

private:
    static bool IsStatusOK(redisReply *pReply);
    static bool GetValue(redisReply *pReply, std::string &strVal);
    static bool GetArray(redisReply *pReply, std::vector<std::string> &vecVal);
    static CRedisServer * FindServer(const std::vector<CRedisServer *> &vecRedisServ, const std::string &strHost, int nPort);

    void CleanServer();
    redisContext * GetConnection(const std::string &strKey);
    CRedisServer * GetMatchedServer(const CRedisCommand *pRedisCmd) const;
    CRedisServer * GetActiveServer() const;
    bool LoadClusterSlots();
    int Excute(CRedisCommand *pRedisCmd, Pipeline ppLine = nullptr);

    template <typename P>
    int ExcuteImpl(const std::string &strCmd, TFuncFetch funcFetch, P tArg, int nSlot,
                   Pipeline ppLine = nullptr, TFuncConvert funcConv = FUNC_DEF_CONV)
    {
        CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, CMD_NORMAL, !ppLine);
        pRedisCmd->SetConvFunc(funcConv);
        pRedisCmd->SetSlot(nSlot);
        pRedisCmd->SetArgs(tArg);
        if (!ppLine)
            pRedisCmd->SetFetchFunc(funcFetch);
        int nRet = Excute(pRedisCmd, ppLine);
        if (!ppLine)
            delete pRedisCmd;
        return nRet;
    }

    template <typename P1, typename P2>
    int ExcuteImpl(const std::string &strCmd, TFuncFetch funcFetch, P1 tArg1, P2 tArg2, int nSlot,
                   Pipeline ppLine = nullptr, TFuncConvert funcConv = FUNC_DEF_CONV)
    {
        CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, CMD_NORMAL, !ppLine);
        pRedisCmd->SetConvFunc(funcConv);
        pRedisCmd->SetSlot(nSlot);
        pRedisCmd->SetArgs(tArg1, tArg2);
        if (!ppLine)
            pRedisCmd->SetFetchFunc(funcFetch);
        int nRet = Excute(pRedisCmd, ppLine);
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

