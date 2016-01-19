#include <stdio.h>
#include "TestBase.hpp"
#include "TestGeneric.hpp"
#include "TestString.hpp"
#include "TestList.hpp"
#include "TestSet.hpp"
#include "TestHash.hpp"
#include "TestConcur.hpp"

int main(int argc, char **argv)
{
    std::string strHost = "10.10.100.1";
    //std::string strHost = "127.0.0.1";

    //CTestBase testBase(strHost);
    //testBase.StartTest();

    //CTestGeneric testKeys(strHost);
    //testKeys.StartTest();

    //CTestString testStr(strHost);
    //testStr.StartTest();

    //CTestList testList(strHost);
    //testList.StartTest();

    //CTestSet testSet(strHost);
    //testSet.StartTest();

    //CTestHash testHash(strHost);
    //testHash.StartTest();

    CTestConcur testConcur(strHost);
    testConcur.StartTest();
    return 0;
}

