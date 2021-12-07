#!/usr/bin/env bash

DIR="$(cd "`dirname "$0"`"; pwd)"
cd $DIR

source ./common-env.sh

MAIN_CLASS="chapter02.flight_data"

${SPARK_HOME}/bin/spark-submit \
 --master yarn \
 --deploy-mode client \
 --driver-cores 1 \
 --driver-memory 1G \
 --executor-cores 2 \
 --executor-memory 1G \
 --class ${MAIN_CLASS} \
 ${JAR_NAME}
