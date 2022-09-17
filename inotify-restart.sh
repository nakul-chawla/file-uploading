#! /usr/bin/env bash
set -e

/start.sh &
PID=$!

# Listen to the directory for any changes
inotifywait -r -m /app -e modify,move,create,delete,attrib |
  while read path action file; do
    # restart the python app if any python changes are detected
    if [[ "$file" =~ .*py$ ]]; then
      # if the PID exists, kill it
      kill -s 0 $PID > /dev/null && kill $PID
      sleep 1
      /start.sh &
      PID=$!
    fi
  done
