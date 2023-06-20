import json

import pika
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError, \
    open_tunnel
from tqdm import tqdm


class Session(object):
    def __init__(self, ssh_host, ssh_port, ssh_login, ssh_password,
                 remote_address, local_address,
                 rabbit_user, rabbit_password):
        if local_address:
            self.server = open_tunnel(
                ssh_address_or_host=(ssh_host, ssh_port),
                ssh_username=ssh_login,
                ssh_password=ssh_password,
                remote_bind_address=remote_address,
                local_bind_address=local_address
            )
            try:
                self.server.start()
            except BaseSSHTunnelForwarderError:
                self.server.start()
        else:
            self.server = None
        self.remote_address = remote_address
        self.rabbit_user = rabbit_user
        self.rabbit_password = rabbit_password

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.server:
            self.server.close()

    def send_dict_to_rabbit(self, queue, list_of_dicts):
        if self.server:
            url = 'amqp://{}:{}@{}:{}'.format(
                            self.rabbit_user,
                            self.rabbit_password,
                            # self.remote_address[0],
                            # self.remote_address[1]
                            self.server.local_bind_address[0],
                            self.server.local_bind_port
                        )
        else:
            url = 'amqp://{}:{}@{}:{}'.format(
                            self.rabbit_user,
                            self.rabbit_password,
                            self.remote_address[0],
                            self.remote_address[1]
                            # self.server.local_bind_address[0],
                            # self.server.local_bind_port
                        )
        with pika.BlockingConnection(
                pika.URLParameters(
                    url=url,
                )
        ) as connection:

            channel = connection.channel()

            channel.queue_declare(queue=queue, durable=True)

            for dict_body in tqdm(
                    list_of_dicts, desc='Отправка сообщений в rabbit'
            ):
                channel.basic_publish(
                    exchange='',
                    routing_key=queue,
                    body=json.dumps(dict_body).encode('utf8')
                )

                # if '107203001001' not in dict_body['identity']:
                #     continue
                #
                # with open(
                #         f"export/{queue}/{dict_body['identity']}.json",
                #         mode='w'
                # ) as output:
                #     json.dump(dict_body, output)

    @classmethod
    def from_config(cls, config):
        if 'local_address' in config:
            return cls(
                config['ssh_host'],
                config['ssh_port'],
                config['ssh_login'],
                config['ssh_password'],
                tuple(config['remote_address']),
                tuple(config['local_address']),
                config['rabbit_user'],
                config['rabbit_password'],
            )
        else:
            return cls(
                None,
                None,
                None,
                None,
                tuple(config['remote_address']),
                None,
                config['rabbit_user'],
                config['rabbit_password'],
            )
