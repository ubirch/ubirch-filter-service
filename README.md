# UBIRCH FILTER SERVICE

The Ubirch Filter Service is a service concerned with checking incoming requests regarding any attacks. 
For now it's only protecting against replay attacks by checking duplications of messages. It should get extended in 
future.

For testing it might get deactivated.

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


