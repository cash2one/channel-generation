#!/bin/bash

if [ ! -d output ]
then
    mkdir output
fi

rm -rf ./output/*

cp ./README ./output
cp -r ./conf ./output
cp -r ./dao ./output
cp -r ./bll ./output
cp -r ./entrance ./output
mkdir data
mkdir ./output/data/movie
mkdir ./output/data/tv
mkdir ./output/data/comic
mkdir ./output/data/show
cp -r ./data/pm_reviewed_channels ./output/data
cp -r ./log ./output


