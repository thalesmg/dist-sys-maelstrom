#!/usr/bin/env bash

CUR_DIR=$(realpath $(dirname $0))
cd $CUR_DIR

bb --classpath src src/echo.bb
