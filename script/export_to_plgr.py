import datetime
from argparse import ArgumentParser
from os import getcwd
from os.path import join

__all__ = [
    'export_to_plgr',
]

import urllib3
from tqdm import tqdm

from config.config import read_config
from logic.iaimportexport import IAImportExport
from send_to_rabbit.send_to_rabbit import Session


def export_to_plgr():
    parser = ArgumentParser(
        description='Инструмент консольного экспорта в КАРЛ.'
    )
    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config_rabbit.yml'))
    parser.add_argument('-s', '--session', required=False)
    parser.add_argument('-p', '--period', required=False)
    # parser.add_argument('-d', '--debug', required=False, action='store_true',
    #                     default=False)

    args = parser.parse_args()

    # basicConfig(level=args.debug and DEBUG or INFO)

    config = read_config(args.config)

    if args.session:
        config['IA']['session'] = args.session

    if args.period:
        config['daily_task_period'] = args.period

    if config['IA'].get('task_date') == 'today':
        config['IA']['task_date'] = str(datetime.date.today())

    with IAImportExport.from_config(config['IA']) as ia:
        with Session.from_config(config['PLGR']) as session:
            for object_type in config['pf-reset-data']:
                if object_type in [2, 3]:
                    reset_date = str(datetime.date.today().replace(day=1))
                else:
                    reset_date = str(datetime.date.today())
                session.send_dict_to_rabbit(
                    'pf-reset-data',
                    [
                        {
                            'identity': f'{reset_date}_{object_type}',
                            'dateStartReset': reset_date,
                            'objectType': object_type
                        }
                    ]
                )
            for queue, method in config['queues'].items():
                tqdm.write(f'Отправка сообщения в очередь {queue}')
                session.send_dict_to_rabbit(
                    queue,
                    getattr(ia, method)(),
                )


if __name__ == '__main__':
    urllib3.disable_warnings()
    export_to_plgr()
