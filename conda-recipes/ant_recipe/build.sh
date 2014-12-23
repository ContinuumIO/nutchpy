#!/bin/bash

mkdir -p $PREFIX/bin/
mkdir -p $PREFIX/opt/ant/
cp -r  $SRC_DIR/* $PREFIX/opt/ant/
rm -rf $PREFIX/opt/ant/manual
chmod -R +x $PREFIX/opt/ant/bin
cd $PREFIX/bin
ln -s ../opt/ant/bin/ant
