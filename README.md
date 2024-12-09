# Inglorious Crypto - Real-Time Data Pipeline

This project implements a **real-time cryptocurrency data pipeline** that collects trade data from the Binance WebSocket API, processes it using **Kafka**, and stores it in **QuestDB** for fast time-series querying and analysis.

## Features

- **Real-time Data Fetching** from the Binance WebSocket for the BTC/USDT trading pair.
- **Kafka Integration** for decoupling data producer and consumer processes.
- **QuestDB Storage** for storing and querying trade data.
- **Written in Rust** for high performance and low-latency processing.

## Architecture Overview

The pipeline consists of the following components:

1. **WebSocket Fetcher** (Rust):
   - Connects to the Binance WebSocket API and listens for real-time trade updates for the BTC/USDT pair.
   - Sends the fetched trade data to a Kafka topic.

2. **Kafka Producer** (Rust):
   - Sends trade data to Kafka after receiving it from the WebSocket fetcher.

3. **Kafka Consumer** (Rust):
   - Consumes the trade data from the Kafka topic.
   - Processes and inserts the data into **QuestDB** for storage and querying.

4. **QuestDB**:
   - A high-performance database optimized for storing and querying time-series data.
   - Stores the incoming trades for fast access via SQL queries.

## Getting Started

Follow these steps to get the project running on your local machine.

### Prerequisites

Before running the project, ensure you have the following installed:

- **Docker**: Used for running Kafka, Zookeeper, and QuestDB in containers.
- **Rust**: The programming language used for the fetcher, producer, and consumer components.
- **Kafka** and **Zookeeper**: These are used as the messaging system for the data pipeline.

### Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yourusername/inglorious_crypto.git
   cd inglorious_crypto

2. **Set up Docker containers**:

   Run the following command to start the containers for Kafka, Zookeeper, and QuestDB:

   ```bash   
   docker-compose up -d
   This will bring up the necessary services in the background.

### Running the project

The project has three main components that need to run simultaneously: Fetcher, Producer, and Consumer.

1. **Build the Rust project**:
   
   Build the entire project using Cargo:

   ```bash   
   cargo build

2. **Run the Fetcher (WebSocket to Kafka)**:
   
   This component connects to the Binance WebSocket and streams the data to Kafka.

   ```bash   
   cargo run -p fetcher

3. **Run the Producer (Kafka Producer)**:
   
   The producer listens to the WebSocket data, processes it, and sends it to Kafka.

   ```bash   
   cargo run -p producer

4. **Run the Consumer (Kafka Consumer to QuestDB)**:
   
   The consumer listens to Kafka, processes the trade data, and inserts it into QuestDB.

   ```bash   
   cargo run -p consumer

### Verifying Data in QuestDB

To verify that the data is being inserted into QuestDB, open the QuestDB web interface:
   
http://localhost:9000

Log in with the default credentials:

- **Username**: `admin`
- **Password**: `quest`

Once logged in, run the following query to see the inserted data:

sql
SELECT * FROM btc_usdt_trades;

## Directory Structure

Here’s an overview of the project directory:

bash
inglorious_crypto/
├── docker/                     # Docker-related files for Kafka, Zookeeper, QuestDB
│   └── docker-compose.yml
├── src/
│   ├── fetcher/                # WebSocket data fetching and publishing to Kafka
│   ├── producer/               # Kafka producer logic
│   └── consumer/               # Kafka consumer and QuestDB insertion logic
├── target/                     # Compiled project files (ignored in Git)
├── .gitignore                  # Git ignore file for excluding unnecessary files
├── Cargo.toml                  # Rust package configuration file
└── README.md                   # This readme file 


### File Descriptions

1. docker-compose.yml: Docker configuration to set up Kafka, Zookeeper, and QuestDB.
2. src/fetcher: Rust code to fetch data from Binance WebSocket.
3. src/producer: Rust code to publish data to Kafka.
4. src/consumer: Rust code to consume data from Kafka and insert it into QuestDB.

## Future Improvements

1. Error Handling: Improve error handling and retries in case of failures.
2. Scalability: Scale the pipeline by adding more Kafka consumers or distributing the system across multiple machines.
3. Visualization: Integrate with real-time visualization tools like Grafana to monitor and visualize the BTC/USDT data.
4. Backtesting: Use the stored data for backtesting cryptocurrency trading strategies.

## Acknowledgements

1. Binance WebSocket API: Provides real-time cryptocurrency market data.
2. Kafka: Used as a message broker to decouple the data producer and consumer.
3. QuestDB: A high-performance time-series database for storing cryptocurrency trade data.
4. Rust: Chosen for its high performance and memory safety.



