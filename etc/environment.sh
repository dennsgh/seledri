#!/bin/sh
touch .env
echo Setting up environment with OS type: $OSTYPE

if [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
    SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
    export CONFIG=$(dirname "$SCRIPT")
    export WORKINGDIR=$(dirname "$CONFIG")
elif [[ "$OSTYPE" == "darwin"* ]]; then
    SCRIPT="$( cd "$( dirname "$0" )" && pwd )"
    export WORKINGDIR=$(dirname "$SCRIPT")
    export CONFIG="$WORKINGDIR/etc"
fi

export DATA="$WORKINGDIR/data"

export FRONTEND_SRC="$WORKINGDIR/frontend/src"
export SRC="$WORKINGDIR/src"

if [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "darwin"* ]]; then
    export PYTHONPATH="$FRONTEND_SRC:$SRC"
elif [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "msys" ]]; then
    export PYTHONPATH="$FRONTEND_SRC:$SRC"
else
    export PYTHONPATH="$PYTHONPATH:$WORKINGDIR/src:$WORKINGDIR/frontend/src"
fi
# Check if ~/.local/bin/dotenv exists
if [ -f "$HOME/.local/bin/dotenv" ]; then
    dotenv_command="$HOME/.local/bin/dotenv"
else
    dotenv_command="dotenv" # Fallback to default dotenv if the specific path does not exist
fi
# Now use the chosen dotenv command
$dotenv_command -f "$WORKINGDIR/.env" set WORKINGDIR "$WORKINGDIR"
$dotenv_command -f "$WORKINGDIR/.env" set CONFIG "$CONFIG"
$dotenv_command -f "$WORKINGDIR/.env" set DATA "$DATA"
$dotenv_command -f "$WORKINGDIR/.env" set PYTHONPATH "$PYTHONPATH"