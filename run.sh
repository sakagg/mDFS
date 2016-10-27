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
DEPENDENCIES="ExternalLibs/protobuf-java-3.0.0.jar:Compiled"
COMPILED_DIR="Compiled"
NUM_DNS=2

echo "Compiling..."

javac -classpath $DEPENDENCIES $CODE_DIR/$PROTO_CODE_DIR/*.java -d $COMPILED_DIR
javac -classpath $DEPENDENCIES $CODE_DIR/$NN_CODE_DIR/*.java -d $COMPILED_DIR
javac -classpath $DEPENDENCIES $CODE_DIR/$DN_CODE_DIR/*.java -d $COMPILED_DIR
javac -classpath $DEPENDENCIES $CODE_DIR/$CLIENT_CODE_DIR/*.java -d $COMPILED_DIR

echo "Compiled."

java -classpath $DEPENDENCIES:$COMPILED_DIR $NN_CODE_DIR/NameNode --numdn $NUM_DNS & # Name Node

i=0
while [ $i -lt $NUM_DNS ]
do
	java -classpath $DEPENDENCIES:$COMPILED_DIR $DN_CODE_DIR/DataNode --nodeid $i & # Data Nodes
	let i=i+1
done

java -classpath $DEPENDENCIES:$COMPILED_DIR $CLIENT_CODE_DIR/Client --numdn $NUM_DNS # Client 
