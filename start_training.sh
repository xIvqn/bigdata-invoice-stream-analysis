#!/bin/bash

# Removes previous files to avoid errors
rm ./clustering ./threshold ./clustering_bisect ./threshold_bisect

# Train KMeans
spark-submit --class es.dmr.uimp.clustering.KMeansClusterInvoices \
    target/scala-2.11/anomalyDetection-assembly-1.0.jar \
    ./src/main/resources/training.csv \
    ./clustering \
    ./threshold

# Train BisectingKMeans
spark-submit --class es.dmr.uimp.clustering.BisectingKMeansClusterInvoices \
    target/scala-2.11/anomalyDetection-assembly-1.0.jar \
    ./src/main/resources/training.csv \
    ./clustering_bisect \
    ./threshold_bisect
