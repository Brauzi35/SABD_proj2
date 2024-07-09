# SABD_proj2
Progetto 2 del corso SABD-Ingegneria informatica magistrale Roma TorVergata

__Authors__

* :man_technologist: Brauzi Valerio (matricola 0333768)
* :woman_technologist: Calcaura Marina (matricola 0334044)

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Docker installed on your machine
- Docker Compose installed on your machine
- Maven installed on your machine
- An internet connection
- At least 15GB avaliable (images + containers)

## Getting Started

### Setting Up the Environment

1. Clone the repository
2. Compile the project to create the .jar file:
    ```sh
    mvn clean package
    ```
   
    

3. Build and run the Docker containers:
    ```sh
    docker compose up 
    ```
    

### Running the Queries

To execute one of the predefined queries (1 to 3), use the following command:
```sh
docker exec jobmanager flink run -c com.sabd2.flink.QueryX /opt/flink/app/flink-module-1.0-SNAPSHOT.jar 
```
To run Query 1:
```sh
docker exec jobmanager flink run -c com.sabd2.flink.Query1 /opt/flink/app/flink-module-1.0-SNAPSHOT.jar 
```
To run Query 2:
```sh
docker exec jobmanager flink run -c com.sabd2.flink.Query2 /opt/flink/app/flink-module-1.0-SNAPSHOT.jar 
```
To run Query 3:
```sh
docker exec jobmanager flink run -c com.sabd2.flink.Query3 /opt/flink/app/flink-module-1.0-SNAPSHOT.jar 
```
