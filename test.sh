#!/bin/bash
unzip $1.zip
if [ $? -ne 0 ]; then
    echo "[ERROR] ZIP File Error. Please fix it!"
    echo
    exit 1
fi
cd $1
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
cd codebase
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
cd rbf
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
make clean
make
cd ../rm
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
make clean
make
./rmtest_create_tables
./rmtest_00
./rmtest_01
./rmtest_02
./rmtest_03
./rmtest_04
./rmtest_05
./rmtest_06
./rmtest_07
./rmtest_08
./rmtest_09
./rmtest_10
./rmtest_11
./rmtest_12
./rmtest_13
./rmtest_13b
./rmtest_14
./rmtest_15
