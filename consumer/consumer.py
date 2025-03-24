import sys

from pika import  BlockingConnection
from pika.connection import ConnectionParameters
from pika.exchange_type import ExchangeType



def callback(channel, method, properties, body: bytes):
    filename = sys.argv[1] if len(sys.argv) > 1 else 'output.txt'
    with open(f'consumer/{filename}', 'a') as file:
        file.write(body.decode() + '\n')
        print(f'Recieve:{body.decode()}')
        channel


def consume_messages():
    conn = BlockingConnection(ConnectionParameters(host='rabbitmq'))
    channel = conn.channel()
    channel.exchange_declare(exchange='publisher', exchange_type=ExchangeType.fanout)
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_bind(exchange='publisher', queue='task_queue')
    channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=True)
    print('aert')

    channel.start_consuming()

if __name__ == '__main__':
    print('asdf')
    consume_messages()
    print('Consume end')