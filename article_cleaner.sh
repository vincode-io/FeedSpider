#!/bin/sh

WORKING_DIR="working_dir"

if [ ! -d $WORKING_DIR ]; then
    mkdir $WORKING_DIR
fi

# Concatenate, clean, and randomize the extracted articles
 cat extracted_articles/* | \
     sed -e "s/\([.\!?,'/()]\)/ \1 /g" | \
     tr "[:upper:]" "[:lower:]" | \
     sort -R > \
     $WORKING_DIR/extracted_articles.txt

# Split the records out into a training group and a validation group
TOTAL_RECORD_COUNT=`wc -l $WORKING_DIR/extracted_articles.txt | awk '{print $1}'`
TRAIN_RECORD_COUNT=`echo $TOTAL_RECORD_COUNT | awk '{print int($0 * .8)}'`
let VALID_RECORD_COUNT=$TOTAL_RECORD_COUNT-$TRAIN_RECORD_COUNT

head -n $TRAIN_RECORD_COUNT $WORKING_DIR/extracted_articles.txt > $WORKING_DIR/train.txt
tail -n $VALID_RECORD_COUNT $WORKING_DIR/extracted_articles.txt > $WORKING_DIR/valid.txt