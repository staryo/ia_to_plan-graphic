from argparse import ArgumentParser
from logging import DEBUG, INFO, basicConfig
from os import getcwd
from os.path import join

__all__ = [
    'export_to_plgr',
]

import urllib3

from config.config import read_config
from send_to_rabbit.send_to_rabbit import Session


def export_to_plgr():
    parser = ArgumentParser(
        description='Инструмент консольного запуска расчета MLP.'
    )
    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config_message.yml'))
    parser.add_argument('-d', '--debug', required=False, action='store_true',
                        default=False)

    args = parser.parse_args()

    basicConfig(level=args.debug and DEBUG or INFO)

    config = read_config(args.config)

    with Session.from_config(config['PLGR']) as session:
        session.send_dict_to_rabbit(
            config['queue'],
            [config['message']],
        )


if __name__ == '__main__':
    urllib3.disable_warnings()
    export_to_plgr()
