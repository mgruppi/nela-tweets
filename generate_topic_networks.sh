#!/bin/bash
mkdir "networks"
mkdir "networks/0.5"
mkdir "networks/0.75"

exclude="realDonaldTrump"

p_threshold=0

#path1="topics/0.5"
path1="topics/0.5_april_20"
path2="topics/0.75"

for f in "$path1"/*
do
  echo "$f"
  output="$(basename $f)"
  output="${output/.txt/.gml}"
  echo "$output"
  python3 network.py "networks/0.5_april_20/network-$output" \
          --rowid "$f"  \
	        --exclude_authors "$exclude" \
	        # --bipartite
	        # --p_threshold 0.00012
done


#for f in "$path2"/*
#do
#  echo "$f"
#  output="$(basename $f)"
#  output="${output/.txt/.gml}"
#  echo "$output"
#  python3 network.py "networks/0.75/network-$output" \
#          --rowid "$f" \
#          --p_threshold="$p_threshold" \
#	        --exclude_authors "$exclude"
#done
