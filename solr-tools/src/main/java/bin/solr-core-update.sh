#!/usr/bin/env bash
set -e
APP_HOME=`cd $(dirname $0); pwd`
CLASSPATH=$APP_HOME/solr-upgrade-1.0-SNAPSHOT-jar-with-dependencies.jar
echo ${CLASSPATH}
exec java -jar ${CLASSPATH} "$@"