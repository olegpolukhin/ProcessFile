#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

PYTHON_BIN=python

${PYTHON_BIN} ${bin}/process.py
