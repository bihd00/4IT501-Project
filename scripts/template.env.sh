#!/usr/bin/bash

# create a copy of this template called "env.sh" in scripts/ directory
# fill in with proper environment variables (lines with export XXX)
# then run source scripts/env.sh from the root directory

export DB_SERVER="your_mssql_server_address"
export DB_NAME="your_sql_server_database_name"

# ...
# do not modify stuff below

source venv/Scripts/activate

cat << EOF >> .env
DB_SERVER=$DB_SERVER
DB_NAME=$DB_NAME
EOF