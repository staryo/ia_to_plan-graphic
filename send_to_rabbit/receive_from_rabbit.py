import sys

import os
import pika
from sshtunnel import SSHTunnelForwarder


def main(hostname, user, password, queue):
    server = SSHTunnelForwarder(
        ssh_address_or_host=('10.17.10.12', 22),
        ssh_username='i.ri.mullanurov',
        ssh_password='VXzDadZ8YZZty',
        remote_bind_address=('10.17.10.21', 5672),
        local_bind_address=(hostname, 15672)
    )

    server.start()

    with pika.BlockingConnection(
            pika.URLParameters(
                url='amqp://{}:{}@{}:{}'.format(
                    user,
                    password,
                    server.local_bind_address[0],
                    server.local_bind_port
                ),
            )
    ) as connection:

        channel = connection.channel()

        channel.queue_declare(queue=queue)

        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body)

        channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()


if __name__ == '__main__':
    try:
        main(
            'localhost',
            'bfg',
            '7XWpwzYhZzzaU5gwXxkXHwN',
            'hello',
        )
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
