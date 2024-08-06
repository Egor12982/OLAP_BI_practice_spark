from confluent_kafka import Producer
import json
import numpy as np
from clickhouse_driver import Client
import json
import os

config = {
    'bootstrap.servers': '127.0.0.1:29092',  # адрес Kafka сервера
    'client.id': 'simple-producer',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'PLAINTEXT'
}

producer = Producer(**config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def send_message(data):
    try:
        # Асинхронная отправка сообщения
        producer.produce('skryl_5th_task', data.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Поллинг для обработки обратных вызовов
    except BufferError:
        print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")


def convert_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    elif isinstance(obj, (int, float, str)):
        return obj
    else:
        raise TypeError(f"Bad type")


current_dir = os.path.dirname(os.path.abspath(__file__))


credentials_path = os.path.join(current_dir, 'my_creds.json')


with open(credentials_path) as json_file:
    connect_settings = json.load(json_file)


ch_local_settings = connect_settings['ch'][0]

# Подключение к ClickHouse
client = Client(
    host=ch_local_settings['host'],
    user=ch_local_settings['user'],
    password=ch_local_settings['password']
)

dataset = client.execute("""select toInt64(office_id), 
                                state_id,
                                toInt64(price_writeoff)
                            from tmp.skryl_writeoffs_by_customers_qty 
                            where office_id != 0
                            limit 100""")

for i in dataset:
    final_dataset = json.dumps(
        {
            'office_id': convert_types(i[0]),
            'state_id': convert_types(i[1]),
            'price_writeoff': convert_types(i[2])
        }
    )
    send_message(final_dataset)

producer.flush()
