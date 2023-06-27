#!/usr/bin/env bash
declare -A log_levels=( [FATAL]=0 [ERROR]=3 [WARNING]=4 [INFO]=6 [DEBUG]=7)
if ! command -v date &> /dev/null
then
    echo "date could not be found"
    exit
fi
if ! command -v jq &> /dev/null
then
    echo "jq could not be found"
    exit
fi
json_logger() {
  log_level=$1
  message=$2
  level=${log_levels[$log_level]}
  timestamp=$(date --rfc-3339=ns  | sed 's/ /T/')
  jq --raw-input --compact-output \
    '{
      "level": "'$log_level'",
      "timestamp": "'$timestamp'",
      "message": .
    }'
}

trap 'catch $? $LINENO' ERR

catch() {
  echo "Error $1 occurred on $2" | json_logger "FATAL"
  trap '' INT TERM
  sleep infinity & pid=$!

  while wait $pid; test $? -ge 128
  do echo 'exiting' | json_logger "INFO"
  done
  exit 1
}
. /app/.venv/bin/activate

export PYTHONPATH=/app/python/lib


# SIGTERM-handler
term_handler() {
# SIGTERM on valid PID; return exit code 0 (clean exit)
  if [ $pid -ne 0 ]; then
    echo Terminating syslog-ng... | json_logger "INFO"
    kill -SIGTERM ${pid}
    wait ${pid}
    exit $?
  fi
# 128 + 15 -- SIGTERM on non-existent process (will cause service failure)
  exit 143
}

# SIGHUP-handler
hup_handler() {
  if [ $pid -ne 0 ]; then
    echo Reloading syslog-ng... | json_logger "WARNING"
    kill -SIGHUP ${pid}
  fi
}

# SIGQUIT-handler
quit_handler() {
  if [ $pid -ne 0 ]; then
    echo Quitting syslog-ng... | json_logger "FATAL"
    kill -SIGQUIT ${pid}
    wait ${pid}
  fi
}

trap 'kill ${!}; hup_handler' SIGHUP
trap 'kill ${!}; term_handler' SIGTERM
trap 'kill ${!}; quit_handler' SIGQUIT

syslog-ng $SYSLOGNG_OPTS -s | json_logger "DEBUG"

if [ "${SYSLOGNG_DUMP_CONFIG}" == "yes" ]
then
  syslog-ng --no-caps --preprocess-into=/tmp/syslog-ng.conf 
  printenv >/tmp/env_file
  export >/tmp/export_file
fi

syslog-ng -s --no-caps
if [ $? != 0 ]
then
  if [ "${DEBUG_CONTAINER}" == "yes" ]
  then
    tail -f /dev/null
  else
    exit $?
  fi
fi

echo starting syslog-ng | json_logger "INFO"
exec syslog-ng $SYSLOGNG_OPTS -F $@
