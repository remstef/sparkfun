#!/bin/bash

HADOOP_CONF_DIR=/etc/hadoop/conf/
YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn/

$class=sparkfun.TestJ8

# testing, shortrunning, longrunnning, default
queue=testing

num_executors=100
mem_driver=2g
mem_executor=1g

jarfile=sparkfun-retroed.jar

params="${1} ${2}"

spark-submit \
--class=$class \
--master=yarn-cluster \
--queue=$queue \
--num-executors $num_executors \
--driver-memory $mem_driver \
--executor-memory $mem_executor \
$jarfile \
$params