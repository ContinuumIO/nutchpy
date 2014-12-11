#!/bin/bash

export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home

mkdir -vp ${PREFIX}/bin;
mkdir -vp ${PREFIX}/lib;
mkdir -vp ${PREFIX}/plugins;
mkdir -vp ${PREFIX}/conf;

# build nutch
ant

pushd runtime/local/
cp -r bin/ ${PREFIX}/bin
cp -r lib/ ${PREFIX}/lib
cp -r plugins/ ${PREFIX}/plugins
cp -r conf/ ${PREFIX}/conf

cp ${RECIPE_DIR}/nutch-site.xml ${PREFIX}/conf/
