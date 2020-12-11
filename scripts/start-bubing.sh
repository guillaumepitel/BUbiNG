#!/bin/bash

if [ -z ${LOG_DIR} ]; then LOG_DIR=/data1/crawling/logs ; fi
if [ -z ${DATA_DIR} ]; then DATA_DIR=/data1/crawling/data ; fi
if [ -z ${XMS} ]; then XMS=16G ; fi
if [ -z ${XMX} ]; then XMX=16G ; fi
if [ -z ${PROPERTIES} ]; then PROPERTIES=./bubing.beta384.properties ; fi
if [ -z ${JMX_PORT} ]; then JMX_PORT=9999 ; fi
if [ -z ${PROMETHEUS_PORT} ]; then PROMETHEUS_PORT=8880 ; fi

LOGBACK_CONFIG=./logback-bubing.xml

AGENT_NAME=$(uname -n)
IP=$(hostname -i | cut -d' ' -f 1)
NODE_ID=$( hostname | sed -e 's/[^0-9]//g' )
NODE_ID=$(( $NODE_ID - 1 ))
echo "node id = $NODE_ID"

XTRA=""
[[ -n $PRIORITY ]] && {
  KEEP_STORE=1
  AGENT_NAME=${AGENT_NAME}-PRIORITY
  XTRAOPT="-p -r ${DATA_DIR}"
  XTRA="priority-"
}

# remove old crawling data
[[ -z ${KEEP_STORE} ]] && rm -rf ${DATA_DIR}
mkdir -p ${DATA_DIR}

[[ -d ${LOG_DIR}/oldies ]] || mkdir ${LOG_DIR}/oldies
find ${LOG_DIR}/oldies -maxdepth 1 -name "${XTRA}bubing*" -delete
find ${LOG_DIR}/oldies -maxdepth 1 -name "${XTRA}console.*" -delete
find ${LOG_DIR} -maxdepth 1 -name "${XTRA}bubing*" -exec mv {} ${LOG_DIR}/oldies/ \;
find ${LOG_DIR} -maxdepth 1 -name "${XTRA}console.*" -exec mv {} ${LOG_DIR}/oldies/ \;

java -server -cp jars/"*":dependencies/"*":extjars/"*":. \
  -Xss256K -Xms${XMS} -Xmx${XMX} \
  -Djava.rmi.server.hostname=127.0.0.1 \
  -Djava.net.preferIPv4Stack=true \
  -Djavax.net.ssl.sessionCacheSize=65536 \
  -Dlogback.configurationFile=${LOGBACK_CONFIG} \
  -Dcom.sun.management.jmxremote.host=127.0.0.1 \
  -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -DBUBING_LOG_DIR=${LOG_DIR} \
  -DBUBING_LOG_FILENAME=${XTRA}bubing \
  -javaagent:./jmx_prometheus_javaagent-0.3.1.jar=${PROMETHEUS_PORT}:jmxexporter.yml \
  it.unimi.di.law.bubing.Agent \
  -h ${IP} -P ${PROPERTIES} -i ${NODE_ID} ${AGENT_NAME} -n $(echo ${XTRAOPT}) 2> ${LOG_DIR}/${XTRA}console.log.err > ${LOG_DIR}/${XTRA}console.log.out 

