#!/bin/sh

WORKING_DIR="working_dir"

if [ ! -d $WORKING_DIR ]; then
    mkdir $WORKING_DIR
fi

# Concatenate, clean, and randomize the extracted articles
 cat $WORKING_DIR/article_extractor_output/* | \
     sed -e "s/\([.\!?,'/()]\)/ \1 /g" | \
     tr "[:upper:]" "[:lower:]" | \
     shuf > \
     $WORKING_DIR/extracted_articles.txt

# Split the records out into a training group and a validation group
awk -v lines=$(wc -l < $WORKING_DIR/extracted_articles.txt | awk '{print $1}') \
    -v fact=0.80 \
    'NR <= lines * fact {print > "$WORKING_DIR/train.txt"; next} {print > "$WORKING_DIR/valid.txt"}' \
    $WORKING_DIR/extracted_articles.txt