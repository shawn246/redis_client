#ifndef REDIS_CLIENT_H
#define REDIS_CLIENT_H

#include "redis/hiredis.h"
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <sstream>
#include <functional>
#include <thread>
#include <condition_variable>
#include <iostream>
#include <algorithm>
#include <pthread.h>
#include <string.h>

#define RC_REPLY_NIL        1
#define RC_SUCCESS          0
#define RC_REPLY_ERR        -1
#define RC_RQST_ERR         -2
#define RC_MOVED_ERR        -3

#define RQST_RETRY_TIMES    3
#define WAIT_RETRY_TIMES    60

typedef std::vector<std::string> VECSTR;

uint32_t HASH_SLOT(const std::string &strKey);

class CRedisServer;
struct SlotRegion
{
    int nStartSlot;
    int nEndSlot;
    std::vector<std::pair<std::string, int> > vecHost;
    CRedisServer *pRedisServ;
};

static inline bool FetchInteger(redisReply *pReply, long &nVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        nVal = pReply->integer;
        return true;
    }
    return false;
}

static inline bool FetchString(redisReply *pReply, std::string &strVal)
{
    if (pReply->type == REDIS_REPLY_STRING || pReply->type == REDIS_REPLY_STATUS)
    {
        strVal.assign(pReply->str, pReply->len);
        return true;
    }
    return false;
}

static inline bool FetchStringArray(redisReply *pReply, std::vector<std::string> &vecStrVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        std::string strVal;
        vecStrVal.clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            if (pReply->element[i]->type == REDIS_REPLY_STRING ||
                pReply->element[i]->type == REDIS_REPLY_STATUS)
                strVal.assign(pReply->element[i]->str, pReply->element[i]->len);
            else if (pReply->element[i]->type == REDIS_REPLY_NIL)
                strVal.clear();
            else
                return false;

            vecStrVal.push_back(strVal);
        }
        return true;
    }
    return false;
}

static inline bool FetchStringMap(redisReply *pReply, std::map<std::string, std::string> &mapStrVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if ((pReply->elements % 2) != 0)
            return false;

        std::string strKey;
        std::string strVal;
        mapStrVal.clear();
        for (size_t i = 0; i < pReply->elements; )
        {
            if (pReply->element[i]->type == REDIS_REPLY_STRING ||
                pReply->element[i]->type == REDIS_REPLY_STATUS)
                strKey.assign(pReply->element[i]->str, pReply->element[i]->len);
            else if (pReply->element[i]->type == REDIS_REPLY_NIL)
                strKey.clear();
            else
                return false;

            if (pReply->element[i + 1]->type == REDIS_REPLY_STRING ||
                pReply->element[i + 1]->type == REDIS_REPLY_STATUS)
                strVal.assign(pReply->element[i + 1]->str, pReply->element[i + 1]->len);
            else if (pReply->element[i + 1]->type == REDIS_REPLY_NIL)
                strVal.clear();
            else
                return false;

            mapStrVal.insert(std::make_pair(strKey, strVal));
        }
        return true;
    }
    return false;
}

static inline bool FetchTime(redisReply *pReply, struct timeval &tmVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (pReply->elements != 2 || pReply->element[0]->type != REDIS_REPLY_STRING ||
            pReply->element[1]->type != REDIS_REPLY_STRING)
            return false;

        tmVal.tv_sec = atol(pReply->element[0]->str);
        tmVal.tv_usec = atol(pReply->element[1]->str);
        return true;
    }
    return false;
}

static inline bool FetchSlot(redisReply *pReply, std::vector<SlotRegion> &vecSlot)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        vecSlot.clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            SlotRegion slotReg;
            redisReply *pSubReply = pReply->element[i];
            if (pSubReply->type != REDIS_REPLY_ARRAY || pSubReply->elements < 3)
                return false;

            slotReg.nStartSlot = pSubReply->element[0]->integer;
            slotReg.nEndSlot = pSubReply->element[1]->integer;
            slotReg.pRedisServ = nullptr;
            for (int i = 2; i < pSubReply->elements; ++i)
            {
                std::string strHost = pSubReply->element[i]->element[0]->str;
                int nPort = pSubReply->element[i]->element[1]->integer;
                slotReg.vecHost.push_back(std::make_pair(strHost, nPort));
            }
            vecSlot.push_back(slotReg);
        }
        return true;
    }
    return false;
}

class CSafeLock
{
public:
    CSafeLock(pthread_rwlock_t *pLock) : m_pLock(pLock), m_bLocked(false) {}
    ~CSafeLock() { Unlock(); }

    inline bool ReadLock() { return (m_bLocked = (pthread_rwlock_rdlock(m_pLock) == 0)); }
    inline bool WriteLock() { return (m_bLocked = (pthread_rwlock_wrlock(m_pLock) == 0)); }
    inline bool TryReadLock() { return (m_bLocked = (pthread_rwlock_tryrdlock(m_pLock) == 0)); }
    inline bool TryWriteLock() { return (m_bLocked = (pthread_rwlock_trywrlock(m_pLock) == 0)); }
    inline void Unlock() { if (m_bLocked) pthread_rwlock_unlock(m_pLock); }

    inline void lock() { WriteLock(); }
    inline void unlock() { Unlock(); }

private:
    pthread_rwlock_t *m_pLock;
    bool m_bLocked;
};

class CRedisError
{
public:
    bool SetErrInfo(int nErrCode, const std::string &strErrMsg = "")
    {
        if (nErrCode == RC_REPLY_ERR && strErrMsg.substr(0, 5) == "MOVED")
            nErrCode = RC_MOVED_ERR;
        m_nErrCode = nErrCode;
        m_strErrMsg = strErrMsg;
        return m_nErrCode == RC_SUCCESS;
    }

    int GetErrCode() const { return m_nErrCode; }
    std::string GetErrMsg() const { return m_strErrMsg; }

protected:
    CRedisError() : m_nErrCode(RC_SUCCESS) {}

protected:
    int m_nErrCode;
    std::string m_strErrMsg;
};

class CRedisCommand : public CRedisError
{
public:
    virtual bool CmdRequest(redisContext *pContext) { return false; }
    virtual bool CmdAppend(redisContext *pContext) { return false; }
    virtual bool CmdReply(redisContext *pContext) { return false; }
    virtual int GetSlot() const { return -1; }
};

template <typename T>
class CRedisReply : public CRedisCommand
{
public:
    CRedisReply(std::function<bool (redisReply *, T &)> funcConv) : m_pReply(nullptr), m_funcConv(funcConv) {}
    ~CRedisReply()
    {
        if (m_pReply)
            freeReplyObject(m_pReply);
    }

    redisReply *GetReply() const { return m_pReply; }
    T & GetResult() { return m_resVal; }
    const T & GetResult() const { return m_resVal; }

    void DumpReply(std::ostream &os) const
    {
        if (!m_pReply)
            os << "no reply" << std::endl;
        else
            os << m_pReply;
    }

public:
    bool ParseReply()
    {
        if (!m_pReply)
            return SetErrInfo(RC_RQST_ERR, "Request failed");
        else if (m_pReply->type == REDIS_REPLY_ERROR)
            return SetErrInfo(RC_REPLY_ERR, m_pReply->str);
        else if (m_pReply->type == REDIS_REPLY_NIL)
        {
            m_bNil = true;
            return true;
        }
        return m_funcConv(m_pReply, m_resVal) ? true : SetErrInfo(RC_REPLY_ERR, "Parse reply failed");
    }

public:
    redisReply *m_pReply;
    T m_resVal;
    bool m_bNil;
    std::function<bool (redisReply *, T &)> m_funcConv;
};

template <typename T>
class CRedisCommandImpl : public CRedisReply<T>
{
public:
    CRedisCommandImpl(const VECSTR &vecParams, std::function<bool (redisReply *, T &)> funcConv, int nSlot = -1)
        : CRedisReply<T>(funcConv), m_pszArgs(nullptr), m_pnArgsLen(nullptr), m_nSlot(nSlot)
    {
        m_nArgs = vecParams.size();
        if (m_nArgs > 0)
        {
            m_pszArgs = new char *[m_nArgs];
            m_pnArgsLen = new size_t[m_nArgs];
            for (size_t i = 0; i < m_nArgs; ++i)
            {
                m_pszArgs[i] = new char[vecParams[i].size()];
                memcpy(m_pszArgs[i], vecParams[i].data(), vecParams[i].size());
                m_pnArgsLen[i] = vecParams[i].size();
            }
        }
    }
    virtual ~CRedisCommandImpl() { ClearArgs(); }

    virtual int GetSlot() const { return m_nSlot; }

    virtual bool CmdRequest(redisContext *pContext)
    {
        if (m_nArgs <= 0)
            return this->SetErrInfo(RC_RQST_ERR, "Invalid parameters");

        if (!pContext)
            return this->SetErrInfo(RC_RQST_ERR, "Invalid connection");

        this->m_pReply = static_cast<redisReply *>(redisCommandArgv(pContext, m_nArgs,
                                             (const char **)m_pszArgs, (const size_t *)m_pnArgsLen));
        return this->ParseReply();
    }

    virtual bool CmdAppend(redisContext *pContext)
    {
        if (m_nArgs <= 0)
            return this->SetErrInfo(RC_RQST_ERR, "Invalid parameters");

        if (!pContext)
            return this->SetErrInfo(RC_RQST_ERR, "Invalid connection");

        int nRet = redisAppendCommandArgv(pContext, m_nArgs, (const char **)m_pszArgs, (const size_t *)m_pnArgsLen);
        return nRet == REDIS_OK ? this->SetErrInfo(RC_SUCCESS) : this->SetErrInfo(RC_RQST_ERR, pContext->errstr);
    }

    virtual bool CmdReply(redisContext *pContext)
    {
        if (!pContext)
            return this->SetErrInfo(RC_RQST_ERR, "Invalid connection");

        if (this->m_pReply)
        {
            freeReplyObject(this->m_pReply);
            this->m_pReply = nullptr;
        }

        int nRet = redisGetReply(pContext, (void **)&(this->m_pReply));
        return nRet == REDIS_OK ? this->ParseReply() : this->SetErrInfo(RC_RQST_ERR, pContext->errstr);
    }

    void DumpArgs(std::ostream &os) const
    {
        os << "total " << m_nArgs << " args" << std::endl;
        for (int i = 0; i < m_nArgs; ++i)
            os << i + 1 << " : " << m_pszArgs[i] << std::endl;
    }

protected:
    void ClearArgs()
    {
        if (m_pszArgs)
        {
            for (int i = 0; i < m_nArgs; ++i)
                delete [] m_pszArgs[i];
            delete [] m_pszArgs;
        }
        if (m_pnArgsLen)
            delete [] m_pnArgsLen;
        m_pszArgs = nullptr;
        m_pnArgsLen = nullptr;
        m_nArgs = 0;
    }

protected:
    int m_nArgs;
    char **m_pszArgs;
    size_t *m_pnArgsLen;
    int m_nSlot;
};

class CRedisCommandAry : public CRedisError
{
public:
    void Append(CRedisCommand *pRedisCmd) { m_vecRedisCmd.push_back(pRedisCmd); }

    int Size() { return m_vecRedisCmd.size(); }
    CRedisCommand * operator[](int nIndex) { return m_vecRedisCmd[nIndex]; }
    template <typename T> CRedisCommandImpl<T> * GetAt(int nIndex, T)
    { return dynamic_cast<CRedisCommandImpl<T> >(m_vecRedisCmd[nIndex]); }

protected:
    std::vector<CRedisCommand *> m_vecRedisCmd;
};
typedef CRedisCommandAry CRedisPipeline;

class CRedisConnection
{
public:
    CRedisConnection(CRedisServer *pRedisServ);

    bool IsValid() { return m_pContext != nullptr; }
    bool ConnRequest(CRedisCommand *pRedisCmd);
    bool ConnRequest(CRedisCommandAry &redisCmdAry);

private:
    bool ConnectToRedis(const std::string &strHost, int nPort, int nTimeout);
    bool Reconnect();

private:
    CRedisServer *m_pRedisServ;
    redisContext *m_pContext;
    time_t m_nUseTime;
};

class CRedisServer
{
    friend class CRedisConnection;
    friend class CRedisClient;
public:
    CRedisServer(const std::string &strHost, int nPort, int nTimeout, int nConnNum);
    virtual ~CRedisServer();

    void SetSlave(const std::string &strHost, int nPort);

    std::string GetHost() const { return m_strHost; }
    int GetPort() const { return m_nPort; }
    bool IsValid() const { return m_queIdleConn.size(); }

    int ServRequest(CRedisCommand *pRedisCmd);
    int ServRequest(CRedisCommandAry &redisCmdAry);

private:
    bool Initialize();
    CRedisConnection *FetchConn();
    void ReturnConn(CRedisConnection *pRedisConn);
    void CleanConn();

private:
    std::string m_strHost;
    int m_nPort;
    int m_nCliTimeout;
    int m_nSerTimeout;
    int m_nConnNum;

    std::queue<CRedisConnection *> m_queIdleConn;
    std::vector<std::pair<std::string, int> > m_vecHosts;
    std::mutex m_mutexConn;
};

class CRedisClient
{
public:
    CRedisClient();
    ~CRedisClient();

    bool Initialize(const std::string &strHost, int nPort, int nTimeout, int nConnNum);
    bool IsCluster() { return m_bCluster; }

    bool Request(CRedisCommand *pRedisCmd);
    bool Request(CRedisPipeline &pRedisPipe);

private:
    void operator()();

    void CleanServer();
    CRedisServer * MatchServer(int nSlot) const;
    CRedisServer * FindServer(int nSlot) const;

    bool LoadSlaveInfo(const std::map<std::string, std::string> &mapInfo);
    bool LoadClusterSlots();
    bool WaitForRefresh();

    bool OnSameServer(const std::string &strKey1, const std::string &strKey2) const;
    bool OnSameServer(const std::vector<std::string> &vecKey) const;

private:
    static bool ConvertInfoToMap(const std::string &strVal, std::map<std::string, std::string> &mapVal);
    static CRedisServer * FindServer(const std::vector<CRedisServer *> &vecRedisServ, const std::string &strHost, int nPort);

private:
    std::string m_strHost;
    int m_nPort;
    int m_nTimeout;
    int m_nConnNum;
    bool m_bCluster;
    bool m_bValid;
    bool m_bExit;

    std::vector<SlotRegion> m_vecSlot;
    std::vector<CRedisServer *> m_vecRedisServ;

#if defined(linux) || defined(__linux) || defined(__linux__)
    pthread_rwlockattr_t m_rwAttr;
#endif
    pthread_rwlock_t m_rwLock;
    std::condition_variable_any m_condAny;
    std::thread *m_pThread;
};

#endif
