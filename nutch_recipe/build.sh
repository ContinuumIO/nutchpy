#!/bin/bash

if [ "$(uname)" == "Darwin" ]; then
    export JAVA_HOME=$(/usr/libexec/java_home)
    export JRE_HOME=${JAVA_HOME}/jre
else
    export JAVA_HOME="/usr/lib/jvm/java-7-openjdk-amd64/"
    export JRE_HOME="/usr/lib/jvm/java-7-openjdk-amd64/jre"
fi

mkdir -vp ${PREFIX}/bin;
mkdir -vp ${PREFIX}/lib/nutch/bin;
mkdir -vp ${PREFIX}/lib/nutch/lib;
mkdir -vp ${PREFIX}/lib/nutch/plugins;
mkdir -vp ${PREFIX}/lib/nutch/conf;

# build nutch
ant

pushd runtime/local/
cp -r bin/* ${PREFIX}/lib/nutch/bin/
cp -r lib/* ${PREFIX}/lib/nutch/lib/
cp -r plugins/* ${PREFIX}/lib/nutch/plugins/
cp -r conf/* ${PREFIX}/lib/nutch/conf/

cp ${RECIPE_DIR}/nutch-site.xml ${PREFIX}/lib/nutch/conf/

pushd "${PREFIX}/bin"

cat > ${PREFIX}/bin/nutch <<EOF
#!/bin/bash

pushd ${PREFIX}/lib/nutch/
./bin/nutch \$@
popd
EOF

cat > ${PREFIX}/bin/crawl<<EOF
#!/bin/bash

pushd ${PREFIX}/lib/nutch/bin/
./bin/crawl \$@
popd
EOF

chmod +x ${PREFIX}/bin/crawl || exit 1;
chmod +x ${PREFIX}/bin/nutch || exit 1;
