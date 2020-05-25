#!/bin/sh

WORKING_DIR="working_dir"

echo "Training the model..."
fasttext supervised -input $WORKING_DIR/train.txt -output $WORKING_DIR/trained_model -lr 1.0 -epoch 25 -wordNgrams 2 -thread 48

echo
echo "Validating the model..."
fasttext test $WORKING_DIR/trained_model.bin $WORKING_DIR/valid.txt
