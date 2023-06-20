from argparse import ArgumentParser
from logging import DEBUG, INFO, basicConfig
from os import getcwd
from os.path import join

__all__ = [
    'export_to_plgr',
]

import urllib3

from config.config import read_config
from logic.erp_fact_from_csv import get_erp_fact_from_csv
from logic.erp_plan_from_csv import get_erp_plan_from_csv
from logic.iaimportexport import IAImportExport
from logic.list_of_dicts_to_json import list_of_dicts_to_json


def export_to_plgr():
    parser = ArgumentParser(
        description='Инструмент консольного экспорта в КАРЛ'
    )
    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config.yml'))
    parser.add_argument('-d', '--debug', required=False, action='store_true',
                        default=False)

    args = parser.parse_args()

    basicConfig(level=args.debug and DEBUG or INFO)

    config = read_config(args.config)

    with IAImportExport.from_config(config['IA']) as ia:

        list_of_dicts_to_json(
            ia.export_ca_spec(),
            config['default_path'],
            config['folders']['spec']
        )
        list_of_dicts_to_json(
            ia.export_ca_phases(),
            config['default_path'],
            config['folders']['ca_phases']
        )
        list_of_dicts_to_json(
            ia.export_ca_equipment(),
            config['default_path'],
            config['folders']['equipment']
        )
        list_of_dicts_to_json(
            ia.export_ca_zapasy(),
            config['default_path'],
            config['folders']['ca_zapasy']
        )
        # list_of_dicts_to_json(
        #     ia.export_pg_wip(),
        #     config['default_path'],
        #     config['folders']['pg_wip']
        # )
        list_of_dicts_to_json(
            ia.export_departments(),
            config['default_path'],
            config['folders']['departments']
        )
        # list_of_dicts_to_json(
        #     get_erp_fact_from_csv('Fact_301.csv', ia),
        #     config['default_path'],
        #     config['folders']['fact']
        # )
        # list_of_dicts_to_json(
        #     get_erp_plan_from_csv('plan.csv', ia),
        #     config['default_path'],
        #     config['folders']['erp_plan']
        # )
        list_of_dicts_to_json(
            ia.export_entities(),
            config['default_path'],
            config['folders']['entities']
        )
        # list_of_dicts_to_json(
        #     ia.export_phases(),
        #     config['default_path'],
        #     config['folders']['phases']
        # )
        # list_of_dicts_to_json(
        #     ia.export_phases_labor(),
        #     config['default_path'],
        #     config['folders']['phases_labor']
        # )
        list_of_dicts_to_json(
            ia.export_ca_operations(),
            config['default_path'],
            config['folders']['ca_operations']
        )
        list_of_dicts_to_json(
            ia.export_ca_routes(),
            config['default_path'],
            config['folders']['routes']
        )
        list_of_dicts_to_json(
            ia.export_ca_wip(),
            config['default_path'],
            config['folders']['ca_wip']
        )
        # list_of_dicts_to_json(
        #     ia.export_bfg_plan(export_type=-1, qty_column='quantityPlanBFG'),
        #     config['default_path'],
        #     config['folders']['bfg_plan']
        # )
        # list_of_dicts_to_json(
        #     ia.export_bfg_plan(export_type=-1, qty_column='quantityPlanERP'),
        #     config['default_path'],
        #     config['folders']['erp_plan']
        # )
        # list_of_dicts_to_json(
        #     ia.export_bfg_plan(export_type=0, qty_column='quantityLaunch'),
        #     config['default_path'],
        #     config['folders']['bfg_exec']
        # )
        list_of_dicts_to_json(
            ia.export_ca_daily_tasks(),
            config['default_path'],
            config['folders']['ca_daily_tasks']
        )


if __name__ == '__main__':
    urllib3.disable_warnings()
    export_to_plgr()
