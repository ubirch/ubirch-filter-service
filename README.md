# UBIRCH FILTER SERVICE

The filter service shall on the one hand prevent duplications of UPPs and on the other hand prevent simple Replay
Attacks.

## Processing

When a UPP is being processed, first the cache is checked for a UPP with the same hash. If this is the case, the UPP
either gets forwarded or rejected (and send to the error topic) depending on the hint of the incoming and the cached
UPP. In any case, the new UPP for that hash is being stored in the cache. In certain cases, if there is no cached UPP
with the same hash, Cassandra is queried to check if there really is no UPP stored with the same hash.

## Install

### Compile

To build the application run the following command

```
    mvn install
```

### Start the service locally

To do so, start a kafka, cassandra and redis instance. 

Cassandra must have a keyspace event_log and a namespace events following the structure in the
EventLog class. A script to create such keyspace is available under the EmbeddedCassandra test trait.

The values under ```src/main/resources/application.base.conf``` must match your local values.

Once all values are set, start the Service object.

## Running the tests

Tests can be started with ```mvn test```.

In order to run, the tests will launch the embedded version of the following services:
* Redis (on port 6379)
* Kafka
* Cassandra (on port 9042)

Redis and Cassandra might fail to start if the ports are already used / not available.

### Break down into end to end tests

The tests try to validate the different parts of the service.

They're all stored in ```src/test/scala/com/ubirch/filter/services/kafka```

* FilterServiceIntegrationTest is the default integration test case
* FilterServiceIntegrationTestWithoutCache test how the system behaves with and without the redis cache connected.
* FilterServiceUnitTests test individual components of the service

The EmbeddedCassandra and TestBase classes are helper class. EmbeddedCassandra is used to launch and stop the 
aforementioned database. TestBase is the general helper that all test files extend.

The other files contain mock classes of the different classes being tested. 

### Misc

Sometimes embedded kafka bugs and throws TimeoutException. Re-launching the test solves this proble.

## Prometheus Metrics

```
  http://localhost:4321/
```

  or
   
```  
   watch -d "curl --silent http://localhost:4321 | grep ubirch"
```


