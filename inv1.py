from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

# Define variables for tracking previous portfolio evaluation
portfolio1 = {
    'IBM': 1300,
    'AAPL': 2200,
    'FB': 1900,
    'AMZN': 2500,
    'GOOG': 1900,
    'TWTR': 2400
}

portfolio2 = {
    'LNKD': 2900,
    'INTC': 2600,
    'AMD': 2100,
    'MSFT': 1200,
    'DELL': 2700,
    'ORCL': 1200
}

# Create Kafka consumer and subscribe to "StockExchange1" topic 

vdszer = lambda x: loads(x.decode('utf-8'))
consumer = KafkaConsumer('StockExchange1', bootstrap_servers=['localhost:9092'],
                         group_id='g1',
                         value_deserializer=vdszer)

# Create Kafka producer to write to "portfolios" topic

vszer = lambda x: dumps(x).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=vszer)


# Evaluate portfolio for each day

# Initialize dictionaries to store portfolio evaluation and other data

port1 = {}
port2 = {}
n_stock1 = {}
n_stock2 = {}

dates = {}
prev_evaluation1 = {}
prev_evaluation2 = {}
n = 0

for message in consumer:
    data = message.value
    if data['TICK'] in portfolio1 or data['TICK'] in portfolio2:
        if data['TS'] not in dates:
            n += 1
            dates[data['TS']] = n
            
    # Calculate evaluation for portfolio1
    if data['TICK'] in portfolio1:
        price = float(data['PRICE'])
        evaluation = price * portfolio1[data['TICK']]
        if data['TS'] in port1:
            port1[data['TS']] += evaluation
            n_stock1[data['TS']] += 1
        else:
            port1[data['TS']] = evaluation
            n_stock1[data['TS']] = 1

        # Check if 5 stocks have been evaluated for the current timestamp
        if n_stock1[data['TS']] == 5:
            n_stock1[data['TS']] += 1

            if dates[data['TS']] == 1:
                # Send the message to the "portfolios" topic
                msg = {'Investor': 'Inv1', 'Portfolio': '1', 'Evaluation': port1[data['TS']],
                       'Difference': "There is no previous evaluation",
                       'Percentage Difference': "There is no previous evaluation",
                       'Timestamp': data['TS']}
                #print(msg)
                producer.send("portfolios", value=msg)
                # This evaluation is the next day's previous evaluation
                prev_evaluation1[dates[data['TS']]] = port1[data['TS']]
            else:
                if dates[data['TS']] - 1 in prev_evaluation1:
                    # Send the message to the "portfolios" topic
                    msg = {'Investor': 'Inv1', 'Portfolio': '1', 'Evaluation': port1[data['TS']],
                           'Difference': port1[data['TS']] - prev_evaluation1[dates[data['TS']] - 1],
                           'Percentage Difference': ((port1[data['TS']] - prev_evaluation1[dates[data['TS']] - 1]) /
                                                     prev_evaluation1[dates[data['TS']] - 1]) * 100,
                           'Timestamp': data['TS']}
                    producer.send("portfolios", value=msg)
                    #print(msg)
                    # This evaluation is the next day's previous evaluation
                    prev_evaluation1[dates[data['TS']]] = port1[data['TS']]
                else:
                    err = "There was an error. Try running the inv.py files first, and then the servers."
                    #print(err)
                    producer.send("portfolios", value=msg)
                    break
    
    # Calculate evaluation for portfolio2
    if data['TICK'] in portfolio2:
        price = float(data['PRICE'])
        evaluation = price * portfolio2[data['TICK']]
        if data['TS'] in port2:
            port2[data['TS']] += evaluation
            n_stock2[data['TS']] += 1
        else:
            port2[data['TS']] = evaluation
            n_stock2[data['TS']] = 1

        # Check if 5 stocks have been evaluated for the current timestamp
        if n_stock2[data['TS']] == 5:
            n_stock2[data['TS']] += 1
            if dates[data['TS']] == 1:
                # Send the message to the "portfolios" topic
                msg = {'Investor': 'Inv1', 'Portfolio': '2', 'Evaluation': port2[data['TS']],
                       'Difference': "There is no previous evaluation",
                       'Percentage Difference': "There is no previous evaluation",
                       'Timestamp': data['TS']}
                #print(msg)
                producer.send("portfolios", value=msg)
                # This evaluation is the next day's previous evaluation
                prev_evaluation2[dates[data['TS']]] = port2[data['TS']]
            else:
                if dates[data['TS']] - 1 in prev_evaluation2:
                    # Send the message to the "portfolios" topic
                    msg = {'Investor': 'Inv1', 'Portfolio': '2', 'Evaluation': port2[data['TS']],
                           'Difference': port2[data['TS']] - prev_evaluation2[dates[data['TS']] - 1],
                           'Percentage Difference': ((port2[data['TS']] - prev_evaluation2[dates[data['TS']] - 1]) /
                                                     prev_evaluation2[dates[data['TS']] - 1]) * 100,
                           'Timestamp': data['TS']}
                    producer.send("portfolios", value=msg)
                    #print(msg)
                    # This evaluation is the next day's previous evaluation
                    prev_evaluation2[dates[data['TS']]] = port2[data['TS']]
                else:
                    err = "There was an error. Try running the inv.py files first, and then the servers."
                    #print(err)
                    producer.send("portfolios", value=msg)
                    break
