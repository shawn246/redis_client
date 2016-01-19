#ifndef TEST_BASE_H
#define TEST_BASE_H

#include "TestClient.hpp"

class CTestBase : public CTestClient
{
public:
    CTestBase(const std::string &strHost);
    bool StartTest();

private:
    bool Test_Get();
    bool Test_Hget();
    bool Test_Hset();
    bool Test_Lpush();
    bool Test_Rpush();
    bool Test_Lrange();
    bool Test_Sadd();
    bool Test_Set();
    bool Test_Smembers();
    bool Test_Zadd();
    bool Test_Zrange();
};

#endif
