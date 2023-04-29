import time
import random
import datetime
from kafka import KafkaProducer

# Define Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

stock = [
    ('IBM', 128.25), ('AAPL', 151.60), ('FB', 184.51), ('AMZN', 93.55),
    ('GOOG', 93.86), ('META', 184.51), ('MSI', 265.96), ('INTC', 25.53),
    ('AMD', 82.11), ('MSFT', 254.15), ('DELL', 38.00), ('ORCL', 88.36)
]


end_date = datetime.datetime.now().date()
weekdays = [0,1,2,3,4]

start_date = datetime.datetime(2000,1,1).date()

import holidays

# Define Greek national holidays
gr_holidays = holidays.GR()

# Iterate over all days since 1-1-2000
for single_date in range((end_date - start_date).days + 1):
    date = start_date + datetime.timedelta(days=single_date)
    # Skip weekends and Greek national holidays
    if date.weekday() >= 5 or date in gr_holidays:
        continue
    # Emit stock prices for the given day
    for s in stock:
        # Send the message to the "StockExchange1" topic
        ticker, price = s
        r = random.random() / 10 - 0.05  # r has values between -0.05 to 0.05
        price *= 1 + r
        timestamp = datetime.datetime.combine(date, datetime.time.min)
        msg = '{{"TICK": "{0}", "PRICE": "{1:.2f}", "TS": "{2}"}}'.format(ticker, price, timestamp)
        #print(msg)
        producer.send('StockExchange1', value = msg.encode())
    time.sleep(2)
