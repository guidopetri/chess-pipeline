#! /usr/bin/sh

cd data
./create_dataset.py
cd ../features
./create_features.py
cd ../models
./model_training.py
./model_prediction.py
cd ../visualization
./create_viz.py
