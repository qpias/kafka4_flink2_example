#!/bin/bash
chmod -R 777 /app
exec "/docker-entrypoint.sh" "$@"
