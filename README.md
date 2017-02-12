# redis-event-store
Redis Adapter for ProophEventStore http://getprooph.org

### CAUTION: This package is in an experimental state. Don´t use it in production yet!


## Run Tests

### with Docker

`$ docker-compose -f docker-compose-tests.yml run -e REDIS_HOST=redis --rm composer run-script tests --timeout 0`
