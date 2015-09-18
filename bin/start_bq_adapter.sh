#!/bin/bash
# Starts the soql-bq-adapter service.
BASEDIR=$(dirname $0)/..
# CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf}
CONFIG=$BASEDIR/soql-server-bq/src/main/resources/reference.conf
JARFILE=$BASEDIR/soql-server-bq/target/scala-2.10/soql-server-bq-assembly-*.jar
if [ ! -e $JARFILE ]; then
  cd $BASEDIR && sbt assembly
fi
java -Dconfig.file=$CONFIG -jar $JARFILE &
