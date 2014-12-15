#!/bin/bash

if [ "$(uname)" == "Darwin" ]; then
    export JAVA_HOME=$(/usr/libexec/java_home)
    export JRE_HOME=${JAVA_HOME}/jre
else
    export JAVA_HOME="/usr/lib/jvm/java"
    export JRE_HOME="/usr/lib/jvm/jre"
fi

mkdir -vp ${PREFIX}/solr_pkg;
mkdir -vp ${PREFIX}/bin;


# cd solr-4.10.2/example/solr/collection1/conf
# mv schema.xml schema.xml.org

# cp ~/anaconda/envs/memex/conf/schema.xml .
# sed 's/^\(.*EnglishPorterFilterFactory.*\)/<!--\1-->/g' schema.xml > tmp
# sed 's/^\(.*protwords.*\)/<!--\1-->/g' tmp > schema.xml

cp -r bin/ ${PREFIX}/bin

SOLR_INSTALL=${PREFIX}/solr_pkg
cp -r example/ ${SOLR_INSTALL}
SOLR_CONF=${SOLR_INSTALL}/solr/collection1/conf

mv ${SOLR_CONF}/schema.xml ${SOLR_CONF}/schema.xml.org
cp ${RECIPE_DIR}/schema.xml ${PREFIX}/solr_pkg/solr/collection1/conf/
