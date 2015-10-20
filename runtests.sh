#!/bin/bash
# run in parallel (4 CPUs) and stop after the first failure
py.test -n 4
#py.test  -n 4 test/functional_tests
#py.test  -n 4 test/unit_tests