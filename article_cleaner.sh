#!/bin/sh

WORKING_DIR="working_dir"

if [ ! -d $WORKING_DIR ]; then
    mkdir $WORKING_DIR
fi

# Concatenate, clean, and randomize the extracted articles
 cat extracted_articles/* | \
     sed -e "s/\([.\!?,'/()]\)/ \1 /g" | \
     tr "[:upper:]" "[:lower:]" | \
     shuf > \
     $WORKING_DIR/extracted_articles.txt

# Split the records out into a training group and a validation group
awk -v lines=$(wc -l < $WORKING_DIR/extracted_articles.txt) \
    -v fact=0.80 \
    'NR <= lines * fact {print > "train.txt"; next} {print > "valid.txt"}' \
    $WORKING_DIR/extracted_articles.txt