#!/bin/bash
set -e

PARENT_DIR=$(dirname $(cd "$(dirname "$0")"; pwd))
CI_DIR="$PARENT_DIR/ci/environment"
DEFAULT_ORIENT_VERSION="2.2.33"

# launch simple instance in debug mode with shell hang up
while [ $# -ne 0 ]; do
  case $1 in
    -h)  #set option "a"
      HANG_UP=true
      shift
      ;;
    *) ODB_VERSION=${1:-"${DEFAULT_ORIENT_VERSION}"} ; shift ;;
    \?) #unrecognized option - show help
      echo "Usage: ./start-ci.sh [-h] [orient-version]" \\n
      exit 2
      ;;
  esac
done

if [[ -z "${ODB_VERSION}" ]]; then
    ODB_VERSION=${DEFAULT_ORIENT_VERSION}
fi

# ---- Start

ODB_DIR="${CI_DIR}/orientdb-community-${ODB_VERSION}"
ODB_LAUNCHER="${ODB_DIR}/bin/server.sh"
ODB_LAUNCHER_SYML="${CI_DIR}/orientdb_current/bin/server.sh"

echo "=== Initializing CI environment ==="

if [ -d "/usr/lib/jvm/java-8-oracle/jre" ]; then
    # Force maven on Travis to use JAVA 8
    export JAVA_HOME="/usr/lib/jvm/java-8-oracle/jre"
fi

echo `java -version`
echo `javac -version`
echo `mvn -version`

cd "$PARENT_DIR"

. "$PARENT_DIR/ci/_bash_utils.sh"

if [ ! -d "$ODB_DIR/bin" ]; then

  # Download and extract OrientDB server
  echo "--- Downloading OrientDB v${ODB_VERSION} ---"
  build ${ODB_VERSION} ${CI_DIR}

  # Ensure that launcher script is executable and copy configurations file
  echo "--- Setting up OrientDB ---"
  chmod +x ${ODB_LAUNCHER}
  chmod -R +rw "${ODB_DIR}/config/"

  if [[ "${ODB_VERSION}" == "1.7.10" ]]; then
    cp ${PARENT_DIR}/ci/orientdb-server-config_1.7.10.xml "${ODB_DIR}/config/orientdb-server-config.xml"
  elif [[ "${ODB_VERSION}" == *"2.1"* ]]; then
    cp ${PARENT_DIR}/ci/orientdb-server-config_2.0.xml "${ODB_DIR}/config/orientdb-server-config.xml"
  elif [[ "${ODB_VERSION}" != *"2.0"* ]]; then
    cp ${PARENT_DIR}/ci/orientdb-server-config.xml "${ODB_DIR}/config/orientdb-server-config.xml"
  else
    cp ${PARENT_DIR}/ci/orientdb-server-config_2.0.xml "${ODB_DIR}/config/orientdb-server-config.xml"
  fi

  cp ${PARENT_DIR}/ci/orientdb-server-log.properties "${ODB_DIR}/config/"

  if [ ! -d "${ODB_DIR}/databases" ]; then
    mkdir ${ODB_DIR}/databases
  fi

else
  echo "!!! Found OrientDB v${ODB_VERSION} in ${ODB_DIR} !!!"
fi

echo "Installing databases: "
echo "cp -a ${PARENT_DIR}/tests/default_databases/GratefulDeadConcerts \"${ODB_DIR}/databases/\""
cp -a ${PARENT_DIR}/tests/default_databases/GratefulDeadConcerts "${ODB_DIR}/databases/"

echo "cp -a ${PARENT_DIR}/tests/default_databases/VehicleHistoryGraph \"${ODB_DIR}/databases/\""
cp -a ${PARENT_DIR}/tests/default_databases/VehicleHistoryGraph "${ODB_DIR}/databases/"

# Configure link to the orientdb_current version
rm -rf ${CI_DIR}/orientdb_current
ln -s ${ODB_DIR} ${CI_DIR}/orientdb_current
chmod +x ${ODB_LAUNCHER_SYML}

# Start OrientDB in background.
echo "--- Starting an instance of OrientDB ---"
if [ -z "${HANG_UP}" ]; then
    sh -c ${ODB_LAUNCHER_SYML} </dev/null &>/dev/null &
    # Wait a bit for OrientDB to finish the initialization phase.
    sleep 5
    printf "\n=== The CI environment has been initialized ===\n"
else
    sh -c ${ODB_LAUNCHER_SYML}
fi
