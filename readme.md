# Kafka

This is a learning project that contains the library Confluent kafka, which is a streaming processing platform. This project contains a producerApp and a ConsumerApp.
The producerApp is responsible for creating the channel subscribed in a topic. Then it generates x number of messages.
The ConsumerApp is the project which will connect to the same channel and will consumer the messages. Then they can be saved on a database or processed in some way.

## Installation

This project required docker installed on the machine.
https://docs.docker.com/get-docker/

For the consumerApp, the database postgreSQL is needed
https://www.postgresql.org/

## Usage

How to run docker with postgreSQL, Zookeeper and kafka
> cd Kafka/ProducerApp/docker
> docker-compose up -d

Generate ProducerApp messages
> cd Kafka/ProducerApp
> dotnet run

Consume messages in ConsumerApp
> cd Kafka/ConsumerApp
> dotnet run

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)