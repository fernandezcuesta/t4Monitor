#!/bin/bash
# run in parallel (4 CPUs)
#py.test -n 4
py.test  -n 4 test/functional_tests
py.test  -n 4 test/unit_tests