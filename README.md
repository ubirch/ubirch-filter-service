# UBIRCH FILTER SERVICE

The Ubirch Filter Service is a service concerned with checking incoming requests regarding any attacks. 
For now it's only protecting against replay attacks by checking duplications of messages. It should get extended in future.

For testing it might get deactivated.

## Install

To build the application run the following command

```
    mvn install
```

## Prometheus Metrics

```
  http://localhost:4321/
```

  or
   
```  
   watch -d "curl --silent http://localhost:4321 | grep ubirch"
```


