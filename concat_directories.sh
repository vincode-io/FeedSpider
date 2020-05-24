#!/bin/bash

for directory in working_dir/wikiextractor_output_articles/*; do
       cat $directory/*.bz2 > $directory.bz2
done