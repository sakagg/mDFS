#!/bin/bash

_terminate() {
	echo "Terminating..."
	kill -9 `pgrep -P $$`
	exit
}

trap _terminate SIGINT SIGTERM SIGKILL

CODE_DIR="src"
NN_CODE_DIR="NameNode"
DN_CODE_DIR="DataNode"
CLIENT_CODE_DIR="Client"
PROTO_CODE_DIR="Proto"
DEPENDENCIES="ExternalLibs/protobuf-java-3.0.0.jar"
COMPILED_DIR="Compiled"
NUM_DNS=3

kill $(ps aux | grep 'NameNode' | awk '{print $2}')
kill $(ps aux | grep 'DataNode' | awk '{print $2}')

# Use -c flag to compile
if [[ $# -ge 1 && "$1" == "-c" ]]; then

    echo "Compiling..."
    javac -classpath $DEPENDENCIES:$CODE_DIR $CODE_DIR/$PROTO_CODE_DIR/*.java -d $COMPILED_DIR
    javac -classpath $DEPENDENCIES:$CODE_DIR $CODE_DIR/$NN_CODE_DIR/*.java -d $COMPILED_DIR
    javac -classpath $DEPENDENCIES:$CODE_DIR $CODE_DIR/$DN_CODE_DIR/*.java -d $COMPILED_DIR
    javac -classpath $DEPENDENCIES:$CODE_DIR $CODE_DIR/$CLIENT_CODE_DIR/*.java -d $COMPILED_DIR
    echo "Compiled."

fi

# Start the RMI registry in the Compiled Directory
echo "Starting RMI Registry"
curdir=`pwd`
cd $COMPILED_DIR
rmiregistry 1099 &
cd $curdir

sleep 10

mkdir -p Data/NameNode
java -classpath $DEPENDENCIES:$COMPILED_DIR $NN_CODE_DIR/NameNode --numdn $NUM_DNS & # Name Node
i=0
while [ $i -lt $NUM_DNS ]
do
	mkdir -p Data/DataNode/$i
	java -classpath $DEPENDENCIES:$COMPILED_DIR $DN_CODE_DIR/DataNode --nodeid $i --numdn $NUM_DNS & # Data Nodes
	let i=i+1
done

java -classpath $DEPENDENCIES:$COMPILED_DIR $CLIENT_CODE_DIR/Client --numdn $NUM_DNS # Client 
