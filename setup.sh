#!/usr/bin/env bash
set -e

if [ $(whoami) == "root" ] && [ "$1" != "--force" ]; then
  echo "It's not recommended to run setup with root"
  echo 'run with --force to ignore'
  exit 1
fi

if [ -z "$VIRTUAL_ENV" ] && [ "$1" != "--force" ]; then
  echo "$0 should be run inside a python virtualenv"
  echo 'run with --force to ignore'
  exit 1
fi

echo 'Installing Python dependencies'
pip install pip setuptools --upgrade
pip install -r requirements.txt
