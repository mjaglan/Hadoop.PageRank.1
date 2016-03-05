#!/bin/bash

if [ $# -ne 3 ]; then
    echo 1>&2 Usage: [PageRank Inputs File Directory]  [Number of Urls]  [Number Of Iterations]
    echo "e.g. ./compileAndExecHadoopPageRank.sh PageRankDataGenerator/input.3000.url.txt 3000 20"
    exit -1
fi

# clean existing compiled class
echo "Clean built java class and jar"
ant clean

# compile your code and shows errors if any
echo "Compiling source code with ant"
ant

if [ -f dist/HadoopPageRankMooc.jar ]
then
    echo "Source code compiled!"
else
    echo "There may be errors in your source code, please check the debug message."
    exit 255
fi

cd /root/software/hadoop-1.1.2/
./MultiNodesOneClickStartUp.sh /root/software/jdk1.6.0_33/ nodes

sleep 10

cd /root/MoocHomeworks/HadoopPageRank/
# run pageRank

hadoop dfs -mkdir input
hadoop dfs -put $1 input
hadoop jar dist/HadoopPageRankMooc.jar indiana.cgl.hadoop.pagerank.HadoopPageRank input output $2 $3

# capture the standard output
rm -rf output
hadoop dfs -get output .

echo "PageRank Finished execution, see output in output/."
