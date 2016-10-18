#!/bin/bash

unit_test='test.py'
main_class='Test'

tests=( \
    'test_workflow_sched' \
)

for t in "${tests[@]}"
do
    echo $t
    python $unit_test $main_class.$t &> $t.out &
    sleep 1
done
