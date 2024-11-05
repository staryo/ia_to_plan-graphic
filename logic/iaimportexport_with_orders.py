import csv
import json
import time
from collections import defaultdict
from datetime import datetime, date, timedelta
from functools import partialmethod
from json import JSONDecodeError
from math import floor
from urllib.parse import urljoin

from requests import Session
from tqdm import tqdm

from utils.list_to_dict import list_to_dict, list_to_defdict
from .base import Base

__all__ = [
    'IAImportExport',
]

from .erp_fact_from_csv import get_erp_fact_from_csv

from .erp_plan_from_csv import get_erp_plan_from_csv
from .raport_from_csv import get_raport_from_csv

_DATETIME_SIMPLE_FORMAT = '%Y-%m-%dT%H:%M:%S'


class IAImportExport(Base):

    def __init__(self, login, password, base_url, erp_fact_csv,
                 erp_plan_csv, phase_name_length, departments_for_pg_plan,
                 task_date, task_time, raport_file, short_phase_name_length,
                 daily_task_period, employee, config,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_url = base_url
        self._login = login
        self._password = password
        self._erp_plan_csv = erp_plan_csv
        self._erp_fact_csv = erp_fact_csv

        self._session = Session()
        self._session.verify = False

        self.phase_name_length = phase_name_length
        self.short_phase_name_length = short_phase_name_length or 0
        self.departments_for_pg_plan = departments_for_pg_plan
        self.task_date = task_date
        self.task_time = task_time
        self.raport_file = raport_file
        self.daily_task_period = daily_task_period
        self.employee = employee

        self.entity_orders = defaultdict(set)
        self.routes_orders = defaultdict(set)
        self.cache = {}
        self.config = config

        self.collect_orders_from_daily_tasks()



    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def _make_url(self, uri):
        return urljoin(self._base_url, uri)

    @staticmethod
    def _make_entity_name(filename, timestamp=datetime.now()):
        return '({}) {}'.format(
            timestamp.strftime(_DATETIME_SIMPLE_FORMAT),
            filename
        )

    def _get_from_rest_collection(self, table):
        if table not in self.cache:
            self.cache[table] = []
            self._perform_login()
            counter = 0
            step = 10000
            if table == 'specification_item':
                order_by = '&order_by=parent_id&order_by=child_id'
            elif table == 'operation_profession':
                order_by = '&order_by=operation_id&order_by=profession_id'
            else:
                order_by = '&order_by=id'
            pbar = tqdm(desc=f'Получение данных из таблицы {table}')
            while True:
                temp = self._perform_get(
                    f'rest/collection/{table}'
                    f'?start={counter}'
                    f'&stop={counter + step}'
                    f'{order_by}'
                )
                pbar.total = temp['meta']['count']
                counter += step
                pbar.update(min(
                    step,
                    temp['meta']['count'] - (counter - step)
                ))
                if table not in temp:
                    break
                self.cache[table] += temp[table]
                if counter >= temp['meta']['count']:
                    break
        return self.cache[table]

    def _get_main_session(self):
        return self._perform_get('action/primary_simulation_session')['data']

    def _perform_json_request(self, http_method, uri, **kwargs):
        url = self._make_url(uri)
        logger = self._logger

        logger.debug('Выполнение {} запроса '
                     'по ссылке {!r}.'.format(http_method, url))

        logger.debug('Отправляемые данные: {!r}.'.format(kwargs))

        response = self._session.request(http_method,
                                         url=url,
                                         **kwargs)
        try:
            response_json = response.json()
        except JSONDecodeError:
            logger.error('Получен ответ на {} запрос по ссылке {!r}: '
                         '{!r}'.format(http_method, url, response))
            raise JSONDecodeError

        logger.debug('Получен ответ на {} запрос по ссылке {!r}: '
                     '{!r}'.format(http_method, url, response_json))
        return response_json

    _perform_get = partialmethod(_perform_json_request, 'GET')

    def _perform_post(self, uri, data):
        return self._perform_json_request('POST', uri, json=data)

    def _perform_put(self, uri, data):
        return self._perform_json_request('PUT', uri, json=data)

    def _perform_action(self, uri_part, **data):
        return self._perform_post(
            '/action/{}'.format(uri_part),
            data=data
        )

    def _perform_login(self):
        return self._perform_action(
            'login',
            data={
                'login': self._login,
                'password': self._password
            },
            action='login'
        )['data']

    def get_phase_with_operation_id(self, operation_id):

        if 'phase_identity' in self.cache:
            return self.cache['phase_identity'].get(operation_id)

        self.cache['phase_identity'] = {}

        entity_routes_dict = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )

        operation_dict = list_to_dict(sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: k['nop']
        ))

        entity_routes_phases_dict = list_to_dict(
            self._get_from_rest_collection('entity_route_phase')
        )

        for op_id, operation in operation_dict.items():
            if operation['entity_route_phase_id'] is None:
                if '(' not in operation['identity']:
                    tqdm.write(f'Не найдена фаза для '
                               f'операции {operation["identity"]}')
                continue
            self.cache['phase_identity'][op_id] = entity_routes_phases_dict[
                operation['entity_route_phase_id']
            ]['identity']

        return self.cache['phase_identity'].get(operation_id)

    def export_entities(self):
        self._perform_login()
        return [
            {
                'identity': f"{order}_{entity['identity']}",
                'name': entity['name'],
                'vendorCode': f"{order}_{entity['identity']}"
            } for entity in self._get_from_rest_collection('entity')
            for order in self.entity_orders[entity['identity']]
        ]

    def export_departments(self):
        self._perform_login()
        departments = self._get_from_rest_collection('department')
        result = [{
            'identity': row['identity'],
            'name': row['name'],
        } for row in departments]
        return result

    def export_ca_equipment(self):
        self._perform_login()
        equipment = self._get_from_rest_collection('equipment_class')
        departments_dict = list_to_dict(
            self._get_from_rest_collection('department')
        )
        report = []
        for row in equipment:
            if row['name'] != 'Контроль':
                report.append({
                    'identity': row['identity'],
                    'className': row['name'],
                })
        for row in departments_dict.values():
            report.append({
                'identity': f"{row['name'].replace('-', '')}777",
                'className': 'Контроль',
            })
        return report

    def export_ca_equipment_new(self):
        self._perform_login()
        equipment = self._get_from_rest_collection('equipment')
        departments_dict = list_to_dict(
            self._get_from_rest_collection('department')
        )
        equipment_class_dict = list_to_dict(
            self._get_from_rest_collection('equipment_class')
        )
        report = []
        for row in equipment:
            equipment_class_dict[row['equipment_class_id']]['department_identity'] = \
            departments_dict[row['department_id']]['identity']
            if row['identity']:
                report.append({
                    'identity': row['identity'],
                    'number': row['identity'],
                    'model': row['name'],
                    'workCenterIdentity': equipment_class_dict[row['equipment_class_id']]['identity'],
                    'departmentIdentity': departments_dict[row['department_id']]['identity'],
                })
            continue
        for row in equipment_class_dict.values():
            if row['name'] in ['Ручные операции']:
                try:
                    report.append({
                        'identity': row['identity'],
                        'number': row['name'],
                        'model': row['name'],
                        'workCenterIdentity': row['identity'],
                        'departmentIdentity': row['department_identity'],
                    })
                except KeyError:
                    print(f"для РЦ {row['identity']} не определено подразделение")

        for row in departments_dict.values():
            report.append({
                'identity': f"{row['name'].replace('-', '')}777",
                'number': 'Контроль',
                'model': 'Контроль',
                'workCenterIdentity': f"{row['name'].replace('-', '')}777",
                'departmentIdentity': row['identity'],
            })
        return report

    def export_ca_spec(self):
        self._perform_login()
        entities_dict = list_to_dict(self._get_from_rest_collection('entity'))
        specification = self._get_from_rest_collection('specification_item')

        spec = defaultdict(list)
        for row in specification:
            for order in self.entity_orders[row['parent_id']['identity']]:
                spec[f"{order}_{entities_dict[row['parent_id']]['identity']}"].append({
                    'assemblyElementIdentity': f"{order}_{row[entities_dict[row['child_id']]['identity']]}",
                    'quantityAssemblyElement': row['amount']
                })

        return [{
            'identity': parent,
            'parentAssemblyElementIdentity': parent,
            'items': spec[parent]
        } for parent in spec]

    def export_ca_routes(self):
        self._perform_login()
        entity_dict = list_to_dict(
            self._get_from_rest_collection('entity')
        )
        routes = self._get_from_rest_collection('entity_route')

        report = []
        for row in routes:
            for order in self.entity_orders[entity_dict[row['entity_id']]['identity']]:
                report.append(
                    {
                        'identity': f"{order}_{row['identity']}",
                        'assemblyElementIdentity': f"{order}_{entity_dict[row['entity_id']]['identity']}",
                        'name': row['identity'],
                    }
                )
                self.routes_orders[row['identity']].add(order)

        return report

    def export_ca_phases(self):

        self._perform_login()
        entity_routes_dict = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )
        operations = sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: k['nop']
        )
        departments_dict = list_to_dict(
            self._get_from_rest_collection('department')
        )

        phase_route = {}
        route_step = {}
        report = []
        unique_identities = set()

        for row in tqdm(operations):
            if 'н' in row['identity'] or 'с' in row['identity']:
                continue
            entity_route_id = row['entity_route_id']
            phase_identity = self.get_phase_with_operation_id(
                row['id']
            )
            if phase_identity is None:
                continue

            dept = departments_dict[row['department_id']]['identity']

            if entity_route_id not in phase_route:
                phase_route[entity_route_id] = phase_identity
                route_step[entity_route_id] = 1

            if phase_route[entity_route_id] != phase_identity:
                phase_route[entity_route_id] = phase_identity
                route_step[entity_route_id] += 1

            if phase_identity not in unique_identities:
                for order in self.routes_orders[entity_routes_dict[row['entity_route_id']]['identity']]:
                    report.append({
                        'identity': f"{order}_{phase_identity}",
                        'technologicalProcessIdentity': f"{order}_{entity_routes_dict[row['entity_route_id']]['identity']}",
                        'name': phase_identity[- self.short_phase_name_length:],
                        'priority': route_step[entity_route_id],
                        'departmentIdentity': dept,
                    })
                unique_identities.add(phase_identity)
            if '-' not in phase_identity:
                if f"{phase_identity[:self.phase_name_length]}_VPSK" not in unique_identities:
                    for order in self.routes_orders[entity_routes_dict[row['entity_route_id']]['identity']]:
                        report.append({
                            'identity': f"{order}_{phase_identity[:self.phase_name_length]}_VPSK",
                            'technologicalProcessIdentity': f"{order}_{entity_routes_dict[row['entity_route_id']]['identity']}",
                            'name': f"{phase_identity[:self.phase_name_length]}_VPSK",
                            'priority': 999,
                            'departmentIdentity': dept,
                        })
                        # print(report[-1])
                unique_identities.add(
                    f"{phase_identity[:self.phase_name_length]}_VPSK"
                )

        return report

    def export_ca_operations(self):

        self._perform_login()
        entity_dict = list_to_dict(
            self._get_from_rest_collection('entity')
        )
        entity_routes_dict = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )
        operations = sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: k['nop']
        )
        departments_dict = list_to_dict(
            self._get_from_rest_collection('department')
        )
        equipment_class_dict = list_to_dict(
            self._get_from_rest_collection('equipment_class')
        )

        operations_filtered = list(
            filter(
                lambda x: ('с' not in x['identity']) and
                          # ('н' not in x['identity']) and
                          (x['identity'][-1:] != 'Ц') and
                          (x['nop'][-2:] != '_1') and
                          (x['identity'][-2:] != 'MH') and
                          (x['identity'][-2:] != 'МН'),
                operations
            )
        )

        report = []
        operation_priority = {}

        for row in tqdm(operations_filtered):
            entity_route_id = row['entity_route_id']
            if entity_route_id not in operation_priority:
                operation_priority[entity_route_id] = 0
            operation_priority[entity_route_id] += 1

            entity_id = entity_routes_dict[entity_route_id]['entity_id']
            equipment_class_id = row['equipment_class_id']
            department_id = row['department_id']

            phase_identity = self.get_phase_with_operation_id(
                row['id']
            )

            if '_' not in row['nop']:
                continue

            if phase_identity is None:
                continue

            operation_identity = '{}_{}'.format(
                phase_identity,
                row['nop'].split('_')[-1]
            )

            for order in self.entity_orders[entity_dict[entity_id]['identity']]:
                if row['setup_time'] != 0:
                    op_name = f"Наладка_{row['name']}"
                    op_num = f"{row['nop']}H"
                    op_time = round(row['setup_time'] / 60 / 60 * 10000) / 10000
                    op_identity = f"{order}_{operation_identity}Н"
                elif row['prep_time'] != 0:
                    op_name = f"ПЗ_{row['name']}"
                    op_num = f"{row['nop']}ПЗ"
                    op_time = round(row['prep_time'] / 60 / 60 * 10000) / 10000
                    op_identity = f"{order}_{operation_identity}ПЗ"
                else:
                    op_name = row['name']
                    op_num = row['nop']
                    op_time = round(row['prod_time'] / 60 / 60 * 10000) / 10000
                    op_identity = f"{order}_{operation_identity}"

                report.append({
                    'identity': op_identity,
                    'transitionIdentity': f"{order}_{phase_identity}",
                    'assemblyElementIdentity': f"{order}_{entity_dict[entity_id]['identity']}",
                    'departmentIdentity':
                        departments_dict[department_id]['identity'],
                    'workCenterIdentity':
                        equipment_class_dict[equipment_class_id]['identity'] if
                        equipment_class_dict[equipment_class_id][
                            'name'] != 'Контроль' else f"{departments_dict[department_id]['name'].replace('-', '')}777",
                    'technologicalProcessIdentity': f"{order}_{entity_routes_dict[row['entity_route_id']]['identity']}",
                    'number': op_num,
                    'priority': operation_priority[entity_route_id],
                    'name': op_name,
                    'pieceTime': op_time
                })

        return report

    def collect_orders_from_daily_tasks(self):
        self._perform_login()
        entities_dict = list_to_dict(
            self._get_from_rest_collection('entity')
        )

        if self.config.get('session'):
            session = self.config.get('session')
        else:
            session = self._get_main_session()

        tqdm.write(f"Получаем сменное задание "
                   f"на {self.daily_task_period} часов из сессии {session}")

        tasks = self._perform_get(
            '/rest/collection/simulation_equipment?order_by=simulation_operation_task_equipment.simulation_operation_task.start_date&asc=true&order_by=id&asc=true&order_by=simulation_operation_task_equipment.simulation_operation_task_id&asc=true&with=equipment&with_strict=false&with=equipment_class&with=simulation_operation_task_equipment&with=department&with=simulation_operation_task_equipment.simulation_operation_task&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch&with=simulation_operation_task_equipment.simulation_operation_task.operation&with=simulation_operation_task_equipment.simulation_operation_task.operation.operation_group&with_strict=false&with=simulation_operation_task_equipment.simulation_operation_task.operation.entity_route&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch.entity&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch.simulation_order_entity_batch&with_strict=false&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch.simulation_order_entity_batch.order&with=simulation_operation_task_equipment.simulation_operation_task.operation.operation_entity_route_phase&with_strict=false&'
            'filter= {{simulation_operation_task_equipment.simulation_operation_task.start_time le {} }} '
            'and {{simulation_session_id eq {} }}'
            'and {{simulation_operation_task_equipment.simulation_operation_task.type in ["0"] }}'.format(
                self.daily_task_period, session)
        )
        order_dict = list_to_dict(tasks['order'])
        operation_dict = list_to_dict(tasks['operation'])
        entity_routes_dict = list_to_dict(tasks['entity_route'])
        department_dict = list_to_dict(self._get_from_rest_collection('department'))
        simulation_entity_batch_dict = list_to_dict(tasks['simulation_entity_batch'])
        simulation_order_entity_batch_dict = list_to_dict(tasks['simulation_order_entity_batch'], column='simulation_entity_batch_id')

        operation_entity_dict = {}
        today = datetime.strftime(datetime.now(), '%Y-%m-%d')
        for row in tasks['simulation_operation_task']:
            entity_route_id = operation_dict[row['operation_id']][
                'entity_route_id']
            nop = operation_dict[row['operation_id']]['nop']
            if self.config['skip_dept'] is not None:
                if department_dict[operation_dict[row['operation_id']]['department_id']]['identity'] in self.config[
                    'skip_dept']:
                    continue
            if self.config['only_dept'] is not None:
                if department_dict[operation_dict[row['operation_id']]['department_id']]['identity'] not in self.config[
                    'only_dept']:
                    continue
            if 'Ц' == operation_dict[row['operation_id']]['identity'][-1:]:
                continue
            if '_1' == operation_dict[row['operation_id']]['nop'][-2:]:
                continue
            if 'МН' == operation_dict[row['operation_id']]['identity'][-2:]:
                continue
            if 'MH' == operation_dict[row['operation_id']]['identity'][-2:]:
                continue
            if 'н' in operation_dict[row['operation_id']]['identity']:
                continue
            if 'с' in operation_dict[row['operation_id']]['identity']:
                continue

            entity_id = entity_routes_dict[entity_route_id]['entity_id']

            phase_identity = self.get_phase_with_operation_id(
                row['operation_id']
            )
            try:
                operation_identity = '{}_{}'.format(
                    phase_identity,
                    nop.split('_')[1]
                )
            except IndexError:
                continue

            operation_entity_dict[operation_identity] = \
                entities_dict[entity_id]['identity']

            try:
                task_date = (datetime.strptime(
                    row['start_date'],
                    '%Y-%m-%dT%H:%M:%S.%f%z'
                ) - timedelta(3 / 24)).replace(tzinfo=None)
            except ValueError:
                task_date = (datetime.strptime(
                    row['start_date'],
                    '%Y-%m-%dT%H:%M:%S%z'
                ) - timedelta(3 / 24)).replace(tzinfo=None)

            task_date = datetime.strftime(task_date, '%Y-%m-%d')
            if task_date < today:
                continue

            # simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch.simulation_order_entity_batch.order
            order = order_dict[
                simulation_order_entity_batch_dict[simulation_entity_batch_dict[
                    row['simulation_entity_batch_id']
                ]['id']]['order_id']
            ]['name'][:9]
            self.entity_orders[entities_dict[entity_id]['identity']].add(order)

    def export_ca_daily_tasks(self):

        self._perform_login()

        entities_dict = list_to_dict(
            self._get_from_rest_collection('entity')
        )

        if self.config.get('session'):
            session = self.config.get('session')
        else:
            session = self._get_main_session()

        tqdm.write(f"Получаем сменное задание "
                   f"на {self.daily_task_period} часов из сессии {session}")

        tasks = self._perform_get(
            '/rest/collection/simulation_equipment?order_by=simulation_operation_task_equipment.simulation_operation_task.start_date&asc=true&order_by=id&asc=true&order_by=simulation_operation_task_equipment.simulation_operation_task_id&asc=true&with=equipment&with_strict=false&with=equipment_class&with=simulation_operation_task_equipment&with=department&with=simulation_operation_task_equipment.simulation_operation_task&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch&with=simulation_operation_task_equipment.simulation_operation_task.operation&with=simulation_operation_task_equipment.simulation_operation_task.operation.operation_group&with_strict=false&with=simulation_operation_task_equipment.simulation_operation_task.operation.entity_route&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch.entity&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch.simulation_order_entity_batch&with_strict=false&with=simulation_operation_task_equipment.simulation_operation_task.simulation_entity_batch.simulation_order_entity_batch.order&with=simulation_operation_task_equipment.simulation_operation_task.operation.operation_entity_route_phase&with_strict=false&'
            'filter= {{simulation_operation_task_equipment.simulation_operation_task.start_time le {} }} '
            'and {{simulation_session_id eq {} }}'.format(
                self.daily_task_period, session)
        )

        order_dict = list_to_dict(tasks['order'])
        simulation_equipment_dict = list_to_dict(tasks['simulation_equipment'])
        simulation_operation_task_equipment_dict = list_to_dict(tasks['simulation_operation_task_equipment'],
                                                                'simulation_operation_task_id')
        operation_dict = list_to_dict(tasks['operation'])
        entity_routes_dict = list_to_dict(tasks['entity_route'])
        department_dict = list_to_dict(self._get_from_rest_collection('department'))
        equipment_dict = list_to_dict(tasks['equipment'])
        equipment_class_dict = list_to_dict(tasks['equipment_class'])
        simulation_entity_batch_dict = list_to_dict(tasks['simulation_entity_batch'])
        simulation_order_entity_batch_dict = list_to_dict(tasks['simulation_order_entity_batch'],
                                                          column='simulation_entity_batch_id')

        report = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(
                    lambda: defaultdict(int)
                )
            )
        )

        report_equipment = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: defaultdict(int))
            )
        )

        operation_entity_dict = {}
        today = datetime.strftime(datetime.now(), '%Y-%m-%d')
        for row in tasks['simulation_operation_task']:
            entity_route_id = operation_dict[row['operation_id']][
                'entity_route_id']
            nop = operation_dict[row['operation_id']]['nop']
            if self.config['skip_dept'] is not None:
                if department_dict[operation_dict[row['operation_id']]['department_id']]['identity'] in self.config[
                    'skip_dept']:
                    continue
            if self.config['only_dept'] is not None:
                if department_dict[operation_dict[row['operation_id']]['department_id']]['identity'] not in self.config[
                    'only_dept']:
                    continue
            if 'Ц' == operation_dict[row['operation_id']]['identity'][-1:]:
                continue
            if '_1' == operation_dict[row['operation_id']]['nop'][-2:]:
                continue
            if 'МН' == operation_dict[row['operation_id']]['identity'][-2:]:
                continue
            if 'MH' == operation_dict[row['operation_id']]['identity'][-2:]:
                continue
            # if 'н' in operation_dict[row['operation_id']]['identity']:
            #     continue
            if 'с' in operation_dict[row['operation_id']]['identity']:
                continue

            entity_id = entity_routes_dict[entity_route_id]['entity_id']

            order = order_dict[
                simulation_order_entity_batch_dict[simulation_entity_batch_dict[
                    row['simulation_entity_batch_id']
                ]['id']]['order_id']
            ]['name'][:9]

            phase_identity = self.get_phase_with_operation_id(
                row['operation_id']
            )
            try:
                if operation_dict[row['operation_id']]['setup_time'] != 0:
                    operation_identity = '{}_{}Н'.format(
                        phase_identity,
                        nop.split('_')[-1]
                    )
                elif operation_dict[row['operation_id']]['prep_time'] != 0:
                    operation_identity = '{}_{}ПЗ'.format(
                        phase_identity,
                        nop.split('_')[-1]
                    )
                else :
                    operation_identity = '{}_{}'.format(
                        phase_identity,
                        nop.split('_')[-1]
                    )
            except IndexError:
                continue

            operation_entity_dict[operation_identity] = \
                entities_dict[entity_id]['identity']

            try:
                task_date = (datetime.strptime(
                    row['start_date'],
                    '%Y-%m-%dT%H:%M:%S.%f%z'
                ) - timedelta(3 / 24)).replace(tzinfo=None)
            except ValueError:
                task_date = (datetime.strptime(
                    row['start_date'],
                    '%Y-%m-%dT%H:%M:%S%z'
                ) - timedelta(3 / 24)).replace(tzinfo=None)

            if task_date.hour >= 12:
                task_time = '19:00:00'
            else:
                task_time = '07:00:00'
            task_date = datetime.strftime(task_date, '%Y-%m-%d')
            if task_date < today:
                continue

            report[order][operation_identity][task_date][task_time] += floor(
                row['entity_amount'] * (row['stop_labor'] or 1)
            ) - floor(row['entity_amount'] * (row['start_labor'] or 0))

            curr_wc = equipment_class_dict[simulation_equipment_dict[
                simulation_operation_task_equipment_dict[row['id']]['simulation_equipment_id']]['equipment_class_id']]

            if curr_wc['name'] in ['Ручные операции']:
                curr_eq = curr_wc['identity']
            elif curr_wc['name'] in ['Контроль']:
                curr_dep = department_dict[simulation_equipment_dict[
                    simulation_operation_task_equipment_dict[row['id']]['simulation_equipment_id']]['department_id']]
                curr_eq = f"{curr_dep['name'].replace('-', '')}777"
            else:
                curr_eq = equipment_dict[simulation_equipment_dict[
                    simulation_operation_task_equipment_dict[row['id']]['simulation_equipment_id']]['equipment_id']][
                    'identity'] \
                    if simulation_equipment_dict[
                    simulation_operation_task_equipment_dict[row['id']]['simulation_equipment_id']][
                    'equipment_id'] else False

            if curr_eq:
                report_equipment[operation_identity][task_date][task_time][curr_eq] += floor(
                    row['entity_amount'] * (row['stop_labor'] or 1)
                ) - floor(row['entity_amount'] * (row['start_labor'] or 0))

            # if operation_dict[row['operation_id']] != 0:
            #     op_name = f"Наладка_{row['name']}"
            #     op_num = f"{row['nop']}H"
            #     op_time = round(row['setup_time'] / 60 / 60 * 10000) / 10000
            #     op_identity = f"{order}_{operation_identity}Н"
            # elif operation_dict[row['operation_id']]['prep_time'] != 0:
            #     op_name = f"ПЗ_{row['name']}"
            #     op_num = f"{row['nop']}ПЗ"
            #     op_time = round(row['prep_time'] / 60 / 60 * 10000) / 10000
            #     op_identity = f"{order}_{operation_identity}ПЗ"
            # else:
            #     op_name = row['name']
            #     op_num = row['nop']
            #     op_time = round(row['prod_time'] / 60 / 60 * 10000) / 10000
            #     op_identity = f"{order}_{operation_identity}"


        # for order in report:
        #     for operation in report[order]:
        #         for task_date in report[order][operation]:
        #             for task_time in report[order][operation][task_date]:
        #                 if operation_dict[operation]['setup_time'] != 0:
        #                     op_identity = f"{order}_{operation}Н"
        #                 elif operation_dict[operation]['prep_time'] != 0:
        #                     op_identity = f"{order}_{operation}ПЗ"
        #                 else:
        #                     op_identity = f"{order}_{operation}"
        #
        #                 result = {f'{task_date}_{task_time}_{op_identity}':
        #                     {
        #                         'identity': f'{task_date}_{task_time}_{op_identity}',
        #                         'operationIdentity': op_identity,
        #                         'assemblyElementIdentity': f"{order}_{operation_entity_dict[operation]}",
        #                         'quantityPlan': report[order][operation][task_date][task_time],
        #                         'dateBegin': task_date,
        #                         'timeBegin': task_time,
        #                         'equipments': [
        #                             {
        #                                 "identity": curr_eq,
        #                                 "quantity": quantity if operation_dict[operation]['setup_time'] == 0 and operation_dict[operation]['prep_time'] == 0 else 1
        #                             } for curr_eq, quantity in report_equipment[operation][task_date][task_time].items()
        #                         ],
        #                     }
        #                 }

        result= {
            f'{order}_{task_date}_{task_time}_{operation}':
                {
                    'identity': f'{order}_{task_date}_{task_time}_{operation}',
                    'operationIdentity': f"{order}_{operation}",
                    'assemblyElementIdentity': f"{order}_{operation_entity_dict[operation]}",
                    'quantityPlan': 1 if 'Н' in operation or 'ПЗ' in operation else report[order][operation][task_date][task_time],
                    'dateBegin': task_date,
                    'timeBegin': task_time,
                    'equipments': [
                        {
                            "identity": curr_eq,
                            "quantity": 1 if 'Н' in operation or 'ПЗ' in operation else quantity
                        } for curr_eq, quantity in report_equipment[operation][task_date][task_time].items()
                    ],
                } for order in report
            for operation in report[order]
            for task_date in report[order][operation]
            for task_time in report[order][operation][task_date]
        }

        try:
            with open(
                    "tasks.csv.bak",
                    'r'
            ) as all_positions:
                data = csv.DictReader(all_positions)
                for row in data:
                    if row['dateBegin'] < today:
                        continue
                    if row['identity'] not in result:
                        result[row['identity']] = {
                            key: value for key, value in row.items()
                        }
                        result[row['identity']]['quantityPlan'] = 0
                        result[row['identity']]['equipments'] = None
        except FileNotFoundError:
            pass

        keys = list(result.values())[0].keys()

        with open(
                "tasks.csv.bak",
                mode='w',
                newline=''
        ) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(list(result.values()))

        return list(dict(sorted(result.items())).values())

    def export_ca_wip(self):

        self._perform_login()

        wip_batches = self._get_from_rest_collection('entity_batch')

        entity_routes = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )

        main_routes = {
            entity_routes[entity_route_id]['entity_id']:
                entity_routes[entity_route_id]['identity']
            for entity_route_id in filter(
                lambda x: entity_routes[x]['alternate'] is False, entity_routes
            )
        }

        cur_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        not_report = defaultdict(float)
        department = {}
        for row in wip_batches:
            if row['operation_id'] is None:
                if row['operation_progress'] == 0:
                    continue
                if row['entity_id'] not in main_routes:
                    continue
                transition_identity = self.get_entity_last_phase(
                    row['entity_id']
                )
            else:
                transition_identity = self.get_phase_with_operation_id(
                    row['operation_id']
                )
            not_report[transition_identity] += row['amount']
            department[transition_identity] = row["identity"][-4:]
        report = [{
            'identity': f'{cur_date}_{key}',
            'departmentIdentity': department[key],
            'transitionIdentity': key,
            'quantity': round(value),
            'dateTime': cur_date
        } for key, value in not_report.items()]

        return report

    def export_ca_zapasy(self):

        self._perform_login()

        wip_batches = self._get_from_rest_collection('entity_batch')

        entity_routes = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )

        departments = list_to_dict(
            self._get_from_rest_collection('department')
        )

        operations = list_to_dict(
            self._get_from_rest_collection('operation')
        )

        entities = list_to_dict(
            self._get_from_rest_collection('entity')
        )

        main_routes = {
            entity_routes[entity_route_id]['entity_id']:
                entity_routes[entity_route_id]['identity']
            for entity_route_id in filter(
                lambda x: entity_routes[x]['alternate'] is False, entity_routes
            )
        }

        report_temp = defaultdict(float)
        for row in wip_batches:
            if row['operation_id'] is None:
                if row['operation_progress'] == 0:
                    continue
                if row['entity_id'] not in main_routes:
                    continue
                transition_identity = self.get_entity_last_phase(
                    row['entity_id']
                )
            else:
                transition_identity = self.get_phase_with_operation_id(
                    row['operation_id']
                )
            try:
                report_temp[
                    f"{transition_identity}|" \
                    f"{departments[operations[row['operation_id']]['department_id']]['identity']}|" \
                    f"{entities[row['entity_id']]['identity']}"
                ] += row['amount']
            except KeyError:
                report_temp[
                    f"{transition_identity}|" \
                    f"02904|" \
                    f"{entities[row['entity_id']]['identity']}"
                ] += row['amount']
                print(f"{row['identity']} - партия отправлена на 02904")
                pass
            # report.append({
            #     'identity': row['identity'],
            #     'departmentIdentity': departments[operations[row['operation_id']]['department_id']]['identity'],
            #     'assemblyElementIdentity': entities[row['entity_id']][
            #         'identity'],
            #     'transitionIdentity': transition_identity,
            #     'quantityAssemblyElement': round(row['amount']),
            # })
            # report.append({
            #     'identity': row['identity'],
            #     'departmentIdentity': row['identity'][-4:],
            #     'assemblyElementIdentity': entities[row['entity_id']][
            #         'identity'],
            #     'transitionIdentity': transition_identity,
            #     'quantityAssemblyElement': round(row['amount']),
            # })
        report = [{
            'identity': key,
            'departmentIdentity': key.split('|')[1],
            'assemblyElementIdentity': key.split('|')[2],
            'transitionIdentity': key.split('|')[0],
            'quantityAssemblyElement': value,

        } for key, value in report_temp.items()]
        return report

    def export_pg_wip(self):
        logger = self._logger
        self._perform_login()

        wip_batches = self._get_from_rest_collection('entity_batch')

        operations = sorted(
            self._get_from_rest_collection('operation'),
            key=lambda x: x['nop']
        )
        last_operations_entity_route_id = {}
        for row in operations:
            last_operations_entity_route_id[row['entity_route_id']] = row

        entity_routes = self._get_from_rest_collection('entity_route')

        main_routes = {
            entity_route['entity_id']: entity_route
            for entity_route in filter(
                lambda x: x['alternate'] is False,
                entity_routes
            )
        }

        not_report = defaultdict(float)
        department = {}
        cur_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        for row in tqdm(wip_batches, desc='Разбор партий НЗП'):
            if row['entity_batch_snapshot_id'] is not None:
                continue
            if row['operation_id'] is None:
                if row['operation_progress'] == 0:
                    continue
                if row['entity_id'] not in main_routes:
                    continue
                transition_identity = self.get_entity_last_phase(
                    row['entity_id'],
                )
            else:
                transition_identity = self.get_phase_with_operation_id(
                    row['operation_id']
                )
            if transition_identity is None:
                logger.info(
                    'Отсутствует маршрут для номенклатуры {}'.format(
                        row['entity_id']
                    )
                )
                continue
            not_report[transition_identity] += row['amount']
            department[transition_identity] = row["identity"][-4:]
        report = [{
            'identity': f'{cur_date[:10]}_{key}',
            # временно подставили какое попало подразделение
            'warehouse': department[key],
            'transitionIdentity': key,
            'quantity': round(value),
            'date': cur_date[:10]
        } for key, value in not_report.items()]

        return report

    def get_entity_last_phase(self, entity_id):
        if 'main_routes' not in self.cache:
            entity_routes = self._get_from_rest_collection(
                'entity_route'
            )
            self.cache['main_routes'] = {
                entity_route['entity_id']: entity_route
                for entity_route in filter(
                    lambda x: x['alternate'] is False,
                    entity_routes
                )
            }

        main_routes = self.cache['main_routes']

        if entity_id not in main_routes:
            return None

        entity_route_id = main_routes[entity_id]['id']

        if 'last_operations_entity_route_id' not in self.cache:

            operations = sorted(
                self._get_from_rest_collection('operation'),
                key=lambda x: x['nop']
            )

            self.cache['last_operations_entity_route_id'] = {}
            for row in operations:
                self.cache[
                    'last_operations_entity_route_id'
                ][row['entity_route_id']] = row

        last_operations_entity_route_id = self.cache[
            'last_operations_entity_route_id'
        ]

        try:
            operation = last_operations_entity_route_id[entity_route_id]
        except IndexError:
            return None

        return self.get_phase_with_operation_id(operation['id'])

    @classmethod
    def from_config(cls, config):
        return cls(
            config['login'],
            config['password'],
            config['url'],
            config.get('erp_fact_csv'),
            config.get('erp_plan_csv'),
            config.get('phase_name_length'),
            config.get('departments_for_bfg_plan') or [],
            config.get('task_date'),
            config.get('task_time'),
            config.get('raport_file'),
            config.get('short_phase_name_length'),
            config.get('daily_task_period'),
            config.get('employee'),
            config
        )
