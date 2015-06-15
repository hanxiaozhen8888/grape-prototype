#dGRAPE  [![Build Status](https://travis-ci.org/yecol/grape.svg?branch=master)](https://travis-ci.org/yecol/grape)


An opensource distributed GRAph Pattern matching Engine.


## Usage

#### Programming
Design your own `Compute` and `IncrementalCompute` functions.

Extend abstract classes in the [Interfaces](https://github.com/yecol/grape/tree/master/src/main/java/inf/ed/grape/interfaces) package to satisfy your computation task.

Use your own program or [Metis](https://github.com/yecol/grape/tree/master/lib/metis-5.1.0) to partition graph data.

#### Configure and Compile
Configure parameters in [config.properties](https://github.com/yecol/grape/blob/master/src/main/resources/config.properties).

Then build GRAPE with maven in project root directory.
```sh
$ mvn package
```
#### Run

Maven packaged 3 seperate runable jar files in ./target directory.

```sh
# launch the coordinator
$ java -Djava.security.policy=security.policy -jar grape-coordinator-0.1.jar
# launch and register worker(s) to the coordinator
$ java -Djava.security.policy=security.policy -jar grape-worker-0.1.jar COORDINATOR_IP
# launch client which sends query to the coordinator
$ java -Djava.security.policy=security.policy -jar grape-client-0.1.jar COORDINATOR_IP
```

## Acknowledgement

- Fast type-specific java collection, FastUtil. http://fastutil.di.unimi.it/
- Graph partitioning lib, Metis. http://glaros.dtc.umn.edu/gkhome/views/metis
