import csv
from collections import defaultdict

from utils.list_to_dict import list_to_dict


def get_erp_fact_from_csv(csv_file, ia):
    with open(csv_file, 'r') as input_file:
        data = list(csv.DictReader(input_file))

    ia._perform_login()

    entity_routes_dict = list_to_dict(
        ia._get_from_rest_collection('entity_route')
    )
    operations = sorted(
        ia._get_from_rest_collection('operation'),
        key=lambda k: (k['entity_route_id'], k['nop'])
    )

    operations_list = defaultdict(list)
    for row in operations:
        route_identity = '{}_{}'.format(
            row['identity'][:ia.phase_name_length],
            entity_routes_dict[row['entity_route_id']]['identity'][
                ia.phase_name_length + 1:]
        )
        operations_list[route_identity].append(row)

    for route in operations_list:
        operations_list[route].sort(
            key=lambda i: i['nop']
        )

    summ = defaultdict(float)
    for row in data:
        if len(operations_list[
            row['CODE'][:ia.phase_name_length + 1]
        ]) == 0:
            continue
        if row['CODE'] != operations_list[
                              row['CODE'][:ia.phase_name_length + 1]
                          ][-1]['identity'][:ia.phase_name_length + 10]:
            continue
        summ['{}|{}'.format(
            row['CODE'][:ia.phase_name_length + 1],
            row['DATE'].replace('.', '-')
        )] += float(row['AMOUNT'])

    report = []
    for identity, amount in summ.items():
        report.append({
            'identity': identity,
            'transitionIdentity': identity.split('|')[0],
            'date': identity.split('|')[1],
            'quantityActual': amount
        })

    return report

