#!/bin/zsh

BUBING_SRC_DIR=$(pwd)
MODULE=$(basename ${BUBING_SRC_DIR})

BUBING_VERSION=$(grep "^version=" ${BUBING_SRC_DIR}/build.properties | cut -d '=' -f 2)

[[ -e ${BUBING_SRC_DIR}/bubing-${BUBING_VERSION}.jar ]] || {
        echo "ERROR: can't find ${BUBING_SRC_DIR}/bubing-${BUBING_VERSION}.jar"
        exit 1
}

OUTPUT_DIR=target/${MODULE}

[[ -d ${OUTPUT_DIR} ]] && rm -rf ${OUTPUT_DIR}
mkdir -p ${OUTPUT_DIR}
cp -r scripts/* ${OUTPUT_DIR}/
chmod u+x ${OUTPUT_DIR}/*.sh
mkdir ${OUTPUT_DIR}/jars
cp ${BUBING_SRC_DIR}/bubing-${BUBING_VERSION}.jar ${OUTPUT_DIR}/jars
pushd ${OUTPUT_DIR}
ln -s ${BUBING_SRC_DIR}/jars/runtime dependencies
ln -s ${BUBING_SRC_DIR}/extjars extjars
touch version.$(cat /dev/urandom | tr -cd 'a-f0-9' | head -c 32)
popd

pushd target
tar zchf ${MODULE}.tar.gz ${MODULE}
scp ${MODULE}.tar.gz babbar@salt.babbar.eu:/srv/salt/crawling/files/dev
popd

