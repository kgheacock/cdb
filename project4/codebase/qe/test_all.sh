#!/bin/bash
set -e # Exit from the script if any command fails.

make clean
make

./qetest_create_index

make clean
make

./qetest_01
./qetest_02
./qetest_03
./qetest_04
./qetest_05
./qetest_06
./qetest_07
./qetest_08
./qetest_09
./qetest_10

