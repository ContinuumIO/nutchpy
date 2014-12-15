#!/bin/bash

if [ "$(uname)" == "Darwin" ]; then
    export JAVA_HOME=$(/usr/libexec/java_home)
    export JRE_HOME=${JAVA_HOME}/jre
else
    export JAVA_HOME="/usr/lib/jvm/java"
    export JRE_HOME="/usr/lib/jvm/jre"
fi

SOLR_INSTALL=${PREFIX}/solr_pkg

mkdir -vp ${PREFIX}/bin;
mkdir -vp ${SOLR_INSTALL};

cp -r bin/* ${PREFIX}/bin
cp -r example/* ${SOLR_INSTALL}

#adding log dir
mkdir -vp ${SOLR_INSTALL}/logs
touch ${SOLR_INSTALL}/logs/empty

SOLR_CONF=${SOLR_INSTALL}/solr/collection1/conf

mv ${SOLR_CONF}/schema.xml ${SOLR_CONF}/schema.xml.org
cp ${RECIPE_DIR}/schema.xml ${SOLR_CONF}/
ls ${SOLR_INSTALL}

