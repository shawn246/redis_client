#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H

#include "redis_client.hpp"
#include <iostream>
#include <unistd.h>
#include <time.h>
#include <cmath>

template <typename T>
bool IsInContainer(const T &container, const typename T::value_type &value)
{
    return std::find(container.begin(), container.end(), value) != container.end();
}

template <> bool IsInContainer(const std::set<std::string> &setVal, const std::string &strVal);

class CTestClient
{
public:
    CTestClient(const std::string &strHost, int nConnNum = 10);

protected:
    static bool PrintResult(const std::string &strCmd, bool bSuccess);
    bool InitStringEnv(int nDel, int nSet);
    bool InitListEnv(int nDel, int nSet);
    bool InitSetEnv(int nDel, int nSet);
    bool InitSortedSetEnv(int nDel, int nSet);
    bool InitHashEnv(int nDel, int nSet);
    bool GetTime(struct timeval &tmVal);

protected:
    CRedisClient m_redis;
};

#endif
