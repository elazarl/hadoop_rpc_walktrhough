= Hadoop 2 RPC mechanism Walktrhough

This repository contains simple hadoop RPC interface, and a Go
implementation that reads an RPC request.

To run the Go server do

    $ cd go_hadoop_rpc
    $ go get code.google.com/p/goprotobuf/proto
    $ go build
    $ ./go_hadoop_rpc -server localhost:5121

To run the Java RPC client

    $ mvn exec:java -Dexec.mainClass=com.github.elazar.hadoop.examples.HadoopRPC -Dexec.args="client"

TO run the Java RPC server

    $ mvn exec:java -Dexec.mainClass=com.github.elazar.hadoop.examples.HadoopRPC -Dexec.args="server"

and to run both client and server on the same process

    $ mvn exec:java -Dexec.mainClass=com.github.elazar.hadoop.examples.HadoopRPC
