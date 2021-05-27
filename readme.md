# database-benchmark

This is a seed project to benchmark you database before you decide which want to use.

You can find out more about this topic fom this blog post [hodo.dev/posts/post-23-database-benchmark](https://hodo.dev/posts/post-23-database-benchmark/)

## How to build and run

1.  Clone the repo 

    ```bash
    git clone https://github.com/gabihodoroaga/database-benchmark.git
    cd database-benchmark
    ```

1.  Run [RediSearch](https://oss.redislabs.com/redisearch/) test

    ```bash
    # start the redis container
    docker run -d --name redis-search-2 -p 6379:6379 redislabs/redisearch:2.0.0
    # star the test
    go run . -redis
    # clean up
    docker rm --force redis-search-2
    ```

1.  Run [PostgreSQL](https://www.postgresql.org) test

    ```bash
    # start postgres container
    docker run --name postgres-test -e POSTGRES_PASSWORD=example -e POSTGRES_USER=postgres -d -p 5432:5432 postgres
    # create the database
    docker exec -it postgres-test psql -U postgres -c 'create database test;'
    # run the test
    go run . -postgres
    # clean up
    docker rm --force postgres-test
    ```

1.  Run [Reindexer](https://github.com/Restream/reindexer) test 

    ```bash
    # start the reindexer container
    docker run -p9088:9088 -p6534:6534 -d reindexer/reindexer
    # star the test
    go run . --reindexer
    ```

## Author 

Gabriel Hodoroaga (hodo.dev)
