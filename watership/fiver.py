import pika
from retry import retry


__all__ = ["connect_to_rabbit", "send_message", "close"]


@retry(pika.exceptions.AMQPConnectionError, delay=5)
def connect_to_rabbit(host, queue):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    return channel


def send_message(channel, queue, message):
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))


def close(channel):
    channel.connection.close()
