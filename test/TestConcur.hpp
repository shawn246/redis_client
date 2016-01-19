#ifndef TEST_CONCUR_H
#define TEST_CONCUR_H

#include "TestClient.hpp"

class CTestConcur : public CTestClient
{
public:
    CTestConcur(const std::string &strHost);
    bool StartTest();

private:
    void Test_GetS();
    void Test_Get();
    void Test_Set();

private:
    bool m_bExit;
    std::mutex m_mutex;
};

#endif
