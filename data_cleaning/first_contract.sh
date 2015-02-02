#!/usr/bin/bash

cd $1
for dd_ in `cat *csv | sort -g | awk -F, '{ print $1 }' | uniq`;do grep $dd_ *csv | sort -k7,7 -rg -t, | head -n1 | sed 's/:/,/' ;done > $2
