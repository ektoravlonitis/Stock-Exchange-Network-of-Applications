from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
from json import loads, dumps
import mysql.connector
from datetime import datetime

db = mysql.connector.connect(user='itc6107', password='itc6107', host='127.0.0.1', database='InvestorsDB');

cursor = db.cursor()

vdszer = lambda x: loads(x.decode('utf-8'))
consumer = KafkaConsumer('portfolios', bootstrap_servers=['localhost:9092'],
                         # group_id='g1',
                         # auto_offset_reset='earliest',
                         # group_id='g1',
                         value_deserializer=vdszer)

for message in consumer:
    # print(message)
    # i += 1
    data = message.value
    dtime = data['Timestamp']
    timestamp = datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
    table = ""

    if data['Investor'] == 'Inv1' and data['Portfolio'] == '1':
        cursor.execute(f"INSERT INTO Inv1_P11(Evaluation, Difference, Percentage, Timestamp)"
                       f" VALUE ({data['Evaluation']}, {data['Difference']}, {data['Percentage Difference']},"
                       f"'{dtime}');")
        print('test11')
        db.commit()

    if data['Investor'] == 'Inv1' and data['Portfolio'] == '2':
        cursor.execute(f"INSERT INTO Inv1_P12(Evaluation, Difference, Percentage, Timestamp)"
                       f" VALUE ({data['Evaluation']}, {data['Difference']}, {data['Percentage Difference']},"
                       f"'{dtime}');")
        print('test12')
        db.commit()

    if data['Investor'] == 'Inv2' and data['Portfolio'] == '1':
        cursor.execute(f"INSERT INTO Inv2_P21(Evaluation, Difference, Percentage, Timestamp)"
                       f" VALUE ({data['Evaluation']}, {data['Difference']}, {data['Percentage Difference']},"
                       f"'{dtime}');")
        print('test21')
        db.commit()

    if data['Investor'] == 'Inv2' and data['Portfolio'] == '2':
        cursor.execute(f"INSERT INTO Inv2_P22(Evaluation, Difference, Percentage, Timestamp)"
                       f" VALUE ({data['Evaluation']}, {data['Difference']}, {data['Percentage Difference']},"
                       f"'{dtime}');")
        print('test22')
        db.commit()

    if data['Investor'] == 'Inv3' and data['Portfolio'] == '1':
        cursor.execute(f"INSERT INTO Inv3_P31(Evaluation, Difference, Percentage, Timestamp)"
                       f" VALUE ({data['Evaluation']}, {data['Difference']}, {data['Percentage Difference']},"
                       f"'{dtime}');")
        print('test31')
        db.commit()

    if data['Investor'] == 'Inv3' and data['Portfolio'] == '2':
        cursor.execute(f"INSERT INTO Inv3_P32(Evaluation, Difference, Percentage, Timestamp)"
                       f" VALUE ({data['Evaluation']}, {data['Difference']}, {data['Percentage Difference']},"
                       f"'{dtime}');")
        print('test32')
        db.commit()

cursor.close()
db.close()



