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

from utils.list_to_dict import list_to_dict
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

        self.config = config

        self.cache = {}

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
        # entities = filter(
        #     lambda x: x['entity_type_id'] in [1, 2, 3],
        #     self._get_from_rest_collection('entity')
        # )
        return [{
            'identity': row['identity'],
            'name': row['name'],
            'vendorCode': row['identity']
        } for row in self._get_from_rest_collection('entity')]

    def export_departments(self):
        self._perform_login()
        departments = self._get_from_rest_collection('department')
        result = [{
            'identity': row['identity'],
            'name': row['name'],
        } for row in departments]
        # result.append({
        #     'identity': '0716',
        #     'name': '71600'
        # })
        return result

    def export_ca_equipment(self):
        self._perform_login()
        equipment = self._get_from_rest_collection('equipment_class')
        return [{
            'identity': row['identity'],
            'className': row['name'],
        } for row in equipment]

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
            if row['identity']:
                report.append({
                    'identity': row['identity'],
                    'number': row['identity'],
                    'model': row['name'],
                    'workCenterIdentity': equipment_class_dict[row['equipment_class_id']]['identity'],
                    'departmentIdentity': departments_dict[row['department_id']]['identity'],
                })
            continue

        return report

    def export_ca_spec(self):
        self._perform_login()
        entities_dict = list_to_dict(self._get_from_rest_collection('entity'))
        specification = self._get_from_rest_collection('specification_item')

        spec = defaultdict(list)
        for row in specification:
            spec[entities_dict[row['parent_id']]['identity']].append({
                'assemblyElementIdentity': entities_dict[
                    row['child_id']
                ]['identity'],
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

        report = [{
            'identity': row['identity'],
            'assemblyElementIdentity': entity_dict[row['entity_id']][
                'identity'],
            'name': row['identity'],
        } for row in routes]

        return report

    def export_phases(self):

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

        prev_phase = {}
        dept_route = {}
        for row in operations:
            entity_route_id = row['entity_route_id']
            phase = self.get_phase_with_operation_id(row['id'])
            if entity_route_id not in dept_route:
                dept_route[entity_route_id] = [None]
                prev_phase[entity_route_id] = None
            dept = departments_dict[row['department_id']]['identity']
            if prev_phase[entity_route_id] != phase:
                dept_route[entity_route_id].append(dept)
                prev_phase[entity_route_id] = phase
        for route in dept_route:
            dept_route[route].append(None)
        report = []
        prev_phase = {}
        route_step = {}
        unique_phases = set()
        for row in tqdm(operations, desc='Формирование отчета для отправки'):
            entity_route_id = row['entity_route_id']
            phase = self.get_phase_with_operation_id(row['id'])
            if phase is None:
                continue
            if '107801008001' in phase:
                a = 1
            if entity_route_id not in prev_phase:
                prev_phase[entity_route_id] = None
                route_step[entity_route_id] = 0
            if prev_phase[entity_route_id] != phase:
                route_step[entity_route_id] += 1
                prev_phase[entity_route_id] = phase
            i = route_step[entity_route_id]
            report_row = {
                'identity': phase,
                'name': phase[- self.short_phase_name_length:],
                'incomingDepartmentIdentity': dept_route[entity_route_id][
                    i - 1],
                'processingDepartmentIdentity': dept_route[entity_route_id][i],
                'outgoingDepartmentIdentity': dept_route[entity_route_id][
                    i + 1],
                'assemblyElementIdentity': entity_dict[
                    entity_routes_dict[
                        row['entity_route_id']
                    ]['entity_id']
                ]['identity']
            }
            if report_row['identity'] not in unique_phases:
                report.append(report_row)
                unique_phases.add(report_row['identity'])

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
                report.append({
                    'identity': phase_identity,
                    'technologicalProcessIdentity':
                        entity_routes_dict[row['entity_route_id']]['identity'],
                    'name': phase_identity[- self.short_phase_name_length:],
                    'priority': route_step[entity_route_id],
                    'departmentIdentity': dept,
                })
                if '-' not in phase_identity:
                    if f"{phase_identity[:self.phase_name_length]}_VPSK" not in unique_identities:
                        report.append({
                            'identity': f"{phase_identity[:self.phase_name_length]}_VPSK",
                            'technologicalProcessIdentity':
                                entity_routes_dict[row['entity_route_id']
                                ]['identity'],
                            'name': f"{phase_identity[:self.phase_name_length]}_VPSK",
                            'priority': 999,
                            'departmentIdentity': dept,
                        })
                        print(report[-1])
                    unique_identities.add(
                        f"{phase_identity[:self.phase_name_length]}_VPSK"
                    )
                unique_identities.add(phase_identity)

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
                          ('н' not in x['identity']) and
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

            report.append({
                'identity': operation_identity,
                'transitionIdentity': phase_identity,
                'assemblyElementIdentity':
                    entity_dict[entity_id]['identity'],
                'departmentIdentity':
                    departments_dict[department_id]['identity'],
                'workCenterIdentity':
                    equipment_class_dict[equipment_class_id]['identity'],
                'technologicalProcessIdentity':
                    entity_routes_dict[row['entity_route_id']]['identity'],
                'number': row['nop'],
                'priority': operation_priority[entity_route_id],
                'name': row['name'],
                'pieceTime': round(row['prod_time'] / 60 / 60 * 10000) / 10000
            })

        return report

    def export_phases_labor(self):

        self._perform_login()

        operations = self._get_from_rest_collection('operation')
        operation_professions = self._get_from_rest_collection(
            'operation_profession'
        )
        professions = list_to_dict(self._get_from_rest_collection('profession'))

        multiplicator = defaultdict(float)
        for row in operation_professions:
            if 'контролер' in professions[row['profession_id']]['identity'].lower():
                continue
            if 'otk' in professions[row['profession_id']]['identity'].lower():
                continue
            multiplicator[row['operation_id']] += row['amount']

        labor_report = defaultdict(float)
        for row in operations:
            phase = self.get_phase_with_operation_id(row['id'])
            labor_report[
                phase
            ] += row['prod_time'] * multiplicator[row['id']]

        return [
            {
                'identity': f'{phase_identity}_{str(date.today())}',
                'transitionIdentity': phase_identity,
                'date': str(date(2022, 9, 1)),
                # 'date': str(date.today()),
                'totalTime': round(
                    labor_report[phase_identity] / 60 / 60,
                    4
                ),
            } for phase_identity in labor_report
        ]

    def export_erp_finish(self):
        return get_erp_plan_from_csv(self._erp_plan_csv, self)

    def export_erp_fact(self):
        return get_erp_fact_from_csv(self._erp_fact_csv, self)

    def export_bfg_launch(self):
        return self.export_bfg_plan(export_type=0, qty_column='quantityLaunch')

    def export_bfg_finish(self):
        return self.export_bfg_plan(export_type=-1,
                                    qty_column='quantityPlanBFG')

    # export_type=0 -- для плана запуска
    # export_type=-1 -- для плана выпуска
    def export_bfg_plan(self, export_type, qty_column):

        if export_type == 0:
            DATEPHRASE = 'start_date'
        else:
            DATEPHRASE = 'stop_date'

        self._perform_login()

        departments_dict = list_to_dict(
            self._get_from_rest_collection('department')
        )

        operations = sorted(
            self._get_from_rest_collection('operation'),
            key=lambda k: (k['entity_route_id'], k['nop'])
        )

        operations_list = defaultdict(list)
        for row in operations:
            if 'н' in row['identity']:
                continue
            phase_identity = self.get_phase_with_operation_id(
                row['id']
            )
            if phase_identity is None:
                continue
            operations_list[phase_identity].append(row)

        for route in operations_list:
            operations_list[route].sort(
                key=lambda i: i['nop']
            )

        tqdm.write(f'Получение расписания работы ресурсов '
                   f'для сессии {self._get_main_session()}')

        tasks = self._perform_get(
            'rest/collection/simulation_operation_task?'
            'order_by=start_time&asc=true&'
            'order_by=id&asc=true&'
            'with=simulation_entity_batch&'
            'filter={{ simulation_entity_batch.simulation_session_id eq {} }} '
            'and {{ start_time le 720}}'
            'and {{ type eq 0 }}'.format(self._get_main_session())
        )

        report = defaultdict(lambda: defaultdict(float))

        for row in tqdm(tasks['simulation_operation_task'], desc='Разбор сменных заданий'):
            # if departments_dict[
            #     operations['department_id']
            # ] not in self.departments_for_pg_plan:
            #     continue
            phase_identity = self.get_phase_with_operation_id(
                row['operation_id']
            )
            try:
                if row['operation_id'] != operations_list[
                    phase_identity
                ][export_type]['id']:
                    continue
            except IndexError:
                print(row['operation_id'])
                continue
            try:
                task_date = (datetime.strptime(
                    row[DATEPHRASE],
                    '%Y-%m-%dT%H:%M:%S.%f%z'
                ) - timedelta(3 / 24)).replace(tzinfo=None)
            except ValueError:
                task_date = (datetime.strptime(
                    row[DATEPHRASE],
                    '%Y-%m-%dT%H:%M:%S%z'
                ) - timedelta(3 / 24)).replace(tzinfo=None)

            today = datetime.today()
            task_date = datetime.strftime(
                max(
                    task_date,
                    today
                ),
                '%Y-%m-%d'
            )

            if phase_identity is None:
                continue

            report[phase_identity][task_date] += row['entity_amount'] * (
                    (row['stop_labor'] or 1) - (row['start_labor'] or 0)
            )

        return [
            {
                'identity': '{}_{}'.format(task_date, phase),
                'transitionIdentity': phase,
                'date': task_date,
                qty_column: report[phase][task_date]
            } for phase in report for task_date in report[phase]
        ]

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
          'and {{simulation_session_id eq {} }}'
          'and {{simulation_operation_task_equipment.simulation_operation_task.type in ["0"] }}'.format(self.daily_task_period, session)
        )

        simulation_equipment_dict = list_to_dict(tasks['simulation_equipment'])
        simulation_operation_task_equipment_dict = list_to_dict(tasks['simulation_operation_task_equipment'], 'simulation_operation_task_id')
        operation_dict = list_to_dict(tasks['operation'])
        entity_routes_dict = list_to_dict(tasks['entity_route'])
        department_dict = list_to_dict(self._get_from_rest_collection('department'))
        equipment_dict = list_to_dict(tasks['equipment'])

        report = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(int)
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
                if department_dict[operation_dict[row['operation_id']]['department_id']]['identity'] in self.config['skip_dept']:
                    continue
            if self.config['only_dept'] is not None:
                if department_dict[operation_dict[row['operation_id']]['department_id']]['identity'] not in self.config['only_dept']:
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

            if task_date.hour >= 12:
                task_time = '19:00:00'
            else:
                task_time = '07:00:00'
            task_date = datetime.strftime(task_date, '%Y-%m-%d')
            if task_date < today:
                continue

            report[operation_identity][task_date][task_time] += floor(
                row['entity_amount'] * (row['stop_labor'] or 1)
            ) - floor(row['entity_amount'] * (row['start_labor'] or 0))

            curr_eq = equipment_dict[simulation_equipment_dict[
                simulation_operation_task_equipment_dict[row['id']]['simulation_equipment_id']]['equipment_id']]['identity'] if simulation_equipment_dict[simulation_operation_task_equipment_dict[row['id']]['simulation_equipment_id']]['equipment_id'] else False

            if curr_eq:
                report_equipment[operation_identity][task_date][task_time][curr_eq] += floor(
                    row['entity_amount'] * (row['stop_labor'] or 1)
                ) - floor(row['entity_amount'] * (row['start_labor'] or 0))

        result = {
            f'{task_date}_{task_time}_{operation}':
            {
                'identity': f'{task_date}_{task_time}_{operation}',
                'operationIdentity': operation,
                'assemblyElementIdentity': operation_entity_dict[operation],
                'quantityPlan': report[operation][task_date][task_time],
                'dateBegin': task_date,
                'timeBegin': task_time,
                'equipments': [
                    {
                        "identity": curr_eq,
                        "quantity": quantity
                    } for curr_eq, quantity in report_equipment[operation][task_date][task_time].items()
                ],
            } for operation in report
            for task_date in report[operation]
            for task_time in report[operation][task_date]
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

    def export_ca_daily_tasks_from_raport(
            self
    ):

        self._perform_login()

        # with open(self.raport_file, 'r', encoding='utf-8') as input_file:
        #     result = list(csv.DictReader(
        #         input_file
        #     ))
        #
        # print(result)

        with open(self.raport_file, 'r', encoding='utf-8') as input_file:
             result = json.load(
                 input_file
            )

        print(result)


        # raport = get_raport_from_csv()

        # operations, entity = self._get_operations_for_phases()

        today = datetime.strftime(datetime.now(), '%Y-%m-%d')

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

    def export_ca_employee_from_file(
            self
    ):

        self._perform_login()

        with open(self.employee, 'r', encoding='utf-8') as input_file:
            result = list(csv.DictReader(
                input_file
            ))

        return(result)

    def _get_operations_for_phases(self):
        route_phase_dict = list_to_dict(
            self._get_from_rest_collection('entity_route_phase')
        )

        entity_dict = list_to_dict(
            self._get_from_rest_collection('entity')
        )

        route_dict = list_to_dict(
            self._get_from_rest_collection('entity_route')
        )

        operations_filtered = list(
            filter(
                lambda x: ('с' not in x['identity']) and
                          ('н' not in x['identity']),
                sorted(
                    self._get_from_rest_collection('operation'),
                    key=lambda k: k['nop']
                )
            )
        )

        result = {}
        # phases_sequence = {}
        entity = {}

        for operation in operations_filtered:
            if operation['entity_route_phase_id'] is None:
                    # print(f'Не распознана фаза для операции {operation["nop"]} '
                    #       f'маршрута {route_dict[operation["entity_route_id"]]}')
                continue
            route_phase = route_phase_dict[
                              operation['entity_route_phase_id']
                          ]['identity'][-self.short_phase_name_length:]

            if route_phase not in result:
                result[route_phase] = []

            operation_identity = '{}_{}'.format(
                route_phase,
                # operation['identity'].split('_')[-1][:-1]
                operation['nop'].split('_')[-1]
            )
            result[route_phase].append(operation_identity)

            entity[route_phase] = entity_dict[
                route_dict[operation['entity_route_id']]['entity_id']
            ]['identity']

        # for operation in operations_filtered:
        #     if operation['entity_route_phase_id'] is None:
        #         print(f'Не распознана фаза для операции {operation["nop"]} '
        #               f'маршрута {route_dict[operation["entity_route_id"]]}')
        #         continue
        #     route_phase = '{}_{}'.format(
        #         route_phase_dict[operation['entity_route_phase_id']][
        #             'identity'][:self.phase_name_length],
        #         route_dict[operation['entity_route_id']]['identity'][self.phase_name_length + 1:]
        #     )
        #     if '108003001011' in route_phase:
        #         a = 1
        #     route = route_dict[operation['entity_route_id']]['identity']
        #     if route not in phases_sequence:
        #         try:
        #             phases_sequence[route] = [
        #                 f'{route[6:self.phase_name_length]}-'
        #                 f'{route.split("_")[1][1]}ZPSK'
        #             ]
        #         except IndexError:
        #             phases_sequence[route] = [
        #                 f'{route[5:self.phase_name_length - 1]}-'
        #                 f'{route.split("_")[2][1]}ZPSK'
        #             ]
        #     if route_phase[
        #        :self.phase_name_length
        #        ] not in phases_sequence[route]:
        #         phases_sequence[route].append(
        #             route_phase[:self.phase_name_length]
        #         )
        #     if route_phase not in result:
        #         result[route_phase] = []
        #     try:
        #         result[route_phase].append(f'{phases_sequence[route][-2]}_'
        #                                    f'{operation["nop"].split("_")[1]}')
        #     except IndexError:
        #         result[route_phase].append(f'{phases_sequence[route][-2]}_'
        #                                    f'{operation["nop"]}')
        #     entity[route_phase] = entity_dict[
        #         route_dict[operation['entity_route_id']]['entity_id']
        #     ]['identity']

        return result, entity

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
        }for key, value in not_report.items()]

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
