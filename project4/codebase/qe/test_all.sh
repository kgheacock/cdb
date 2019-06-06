#!/bin/bash
set -e # Exit from the script if any command fails.

make clean
make

echo
echo "============================================"
echo "Testing create/destroy index"
echo "============================================"

./qetest_create_index
./qetest_destroy_index

echo "============================================"

echo
sleep 2

make clean
make

echo
echo "============================================"
echo "Testing standard qetests"
echo "============================================"

./qetest_01
./qetest_02
./qetest_03
./qetest_04
./qetest_05
./qetest_06
./qetest_09
./qetest_10
./qetest_11

echo "============================================"
echo "Successfully returned from all tests"
echo "============================================"
