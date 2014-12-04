#!/bin/bash

set -xe

### The dir for the package script
MY_DIR=$( dirname $0 )
cd $MY_DIR
rm -fr *.deb

### Name of the package, project, etc
NAME=krux-kafka-mirror-maker

### The directory that the build is done in
TARGET=target

### List of files to package
FILES=krux-kafka-mirror-maker-full.jar

### package version
### XXX surely there's a maven command or something?
VERSION=$(grep version pom.xml | head -1 | perl -pe 's/[^\d.]//g')
PACKAGE_VERSION=$VERSION~krux$( date -u +%Y%m%d%H%M )
PACKAGE_NAME=$NAME

### Where this package will be installed
DEST_DIR="/usr/local/${NAME}/"

### Where the sources live
SOURCE_DIR="${MY_DIR}/${TARGET}"

# run fpm
/usr/local/bin/fpm -s dir -t deb -a all -n $PACKAGE_NAME -v $PACKAGE_VERSION --prefix $DEST_DIR -C $SOURCE_DIR $FILES
