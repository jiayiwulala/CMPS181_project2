cmake_minimum_required(VERSION 3.10)
project(codebase)

set(CMAKE_CXX_STANDARD 11)

include_directories(rbf)
include_directories(rm)

add_executable(codebase
        rbf/pfm.cc
        rbf/pfm.h
        rbf/rbfm.cc
        rbf/rbfm.h
        rbf/rbftest1.cc
        rbf/rbftest10.cc
        rbf/rbftest11.cc
        rbf/rbftest12.cc
        rbf/rbftest2.cc
        rbf/rbftest3.cc
        rbf/rbftest4.cc
        rbf/rbftest5.cc
        rbf/rbftest6.cc
        rbf/rbftest7.cc
        rbf/rbftest8.cc
        rbf/rbftest8b.cc
        rbf/rbftest9.cc
        rbf/test_util.h
        rm/rm.cc
        rm/rm.h
        rm/rm_test_util.h
        rm/rmtest_00.cc
        rm/rmtest_01.cc
        rm/rmtest_02.cc
        rm/rmtest_03.cc
        rm/rmtest_04.cc
        rm/rmtest_05.cc
        rm/rmtest_06.cc
        rm/rmtest_07.cc
        rm/rmtest_08.cc
        rm/rmtest_09.cc
        rm/rmtest_10.cc
        rm/rmtest_11.cc
        rm/rmtest_12.cc
        rm/rmtest_13.cc
        rm/rmtest_13b.cc
        rm/rmtest_14.cc
        rm/rmtest_15.cc
        rm/rmtest_create_tables.cc
        rm/rmtest_delete_tables.cc)
