# Docker files

You can use the files in this folder to run the data service 
and required dependencies.

It currently only supports LDS running on the Neo4J backend.

Build the image:
```
docker build -t lds .
```

Run the service:
```
docker-compose up
```