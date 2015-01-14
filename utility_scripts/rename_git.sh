#!/usr/bin/bash

IFS='
'
set -f
for file in $(find "/home/cvdev/replicate/stratdev/" -name *.py)
do
  #echo "$file"
  git mv `$file` `$file`x
done
