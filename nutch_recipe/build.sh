#!/bin/bash

if [ "$(uname)" == "Darwin" ]; then
    export JAVA_HOME=$(/usr/libexec/java_home)
    export JRE_HOME=${JAVA_HOME}/jre
else
    export JAVA_HOME="/usr/lib/jvm/java-7-openjdk-amd64/"
    export JRE_HOME="/usr/lib/jvm/java-7-openjdk-amd64/jre"
fi

mkdir -vp ${PREFIX}/bin;
mkdir -vp ${PREFIX}/lib;
mkdir -vp ${PREFIX}/plugins;
mkdir -vp ${PREFIX}/conf;

# build nutch
ant

pushd runtime/local/
cp -r bin/* ${PREFIX}/bin/
cp -r lib/* ${PREFIX}/lib/
cp -r plugins/* ${PREFIX}/plugins/
cp -r conf/* ${PREFIX}/conf/

cp ${RECIPE_DIR}/nutch-site.xml ${PREFIX}/conf/
