# This is a Stock Exchange Network of Applications
 
This is a network of financial and investment services of stock exchanges and investors as well as applications that analyze those data. To run the network of applications, follow the instructions below.

Firstly, we create the network of financial and investment services of stock exchanges and investors.

## Network

### Prerequisites

Before running the network of applications, you need to have Zookeeper and Kafka installed on your system. If you haven't installed them yet, you can follow these instructions:
Install Zookeeper: sudo apt-get install zookeeperd
Install Kafka: sudo apt-get install kafka


### Configuration

Before starting the network of applications, you need to configure it. To do so, delete the meta.properties file in the ~/kafka/logs/ directory:
sudo rm ~/kafka/logs/meta.properties

Next, delete the topics in case they were created from a previous run:
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic StockExchange1
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic portfolios

Then, create the topics:
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic StockExchange1
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic portfolios


### Starting the Network

To start the network of applications, follow the steps below:

Start Zookeeper: sudo systemctl start zookeeper
Start Kafka: sudo systemctl start kafka
Start the consumer client to receive messages from the topics:
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic StockExchange1 --from-beginning
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic portfolios --from-beginning

Run the investor applications in separate terminal windows:
python3 inv1.py
python3 inv2.py
python3 inv3.py
Alternatively, you can run the investor applications in Pycharm.

Run the stock exchange server applications in separate terminal windows:
python3 se1_server.py
python3 se2_server.py

You can observe the results of the simulation in the terminal windows where the consumer clients are running.


### Stopping the Network

To stop the network of applications, follow the steps below:

Stop the investor applications by pressing Ctrl+C in their respective terminal windows.
Stop the stock exchange server applications by pressing Ctrl+C in their respective terminal windows.
Stop Kafka: sudo systemctl stop kafka
Stop Zookeeper: sudo systemctl stop zookeeper

## Applications

### Next we create the applications that analyze the data

All files can be ran in terminal or a python IDE.

First, run investorsDB.py to generate the database

Next, app1.py, to fill the database with the topic JSON output

Finally, adjust StartApp() to your desired investor (1, 2 or 3)
Run app2.py
