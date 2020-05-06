#!/usr/bin/env bash
python pipeline.py de all
python cd_pipeline.py de preprocess_graph cluster cluster_evolution_graph --snapshots all --pp-decay 0.75 --pp-merge 38000 --pp-ratio 20 --seed 1
python cd_pipeline.py de preprocess_graph cluster cluster_evolution_graph --snapshots all --pp-merge 38000 --pp-ratio 20 --seed 1
