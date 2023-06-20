import csv


def get_erp_plan_from_csv(csv_file, ia):
    with open(csv_file, 'r', encoding='UTF-8') as input_file:
        data = csv.DictReader(input_file)
        report = [{
            'identity': '{}_{}'.format(row['CODE'], row['DATE_TO']),
            'transitionIdentity': '{}_'.format(row['CODE']),
            'date': row['DATE_TO'].replace('.', '-'),
            'quantityPlanERP': int(row['AMOUNT'] or 0),
        } for row in data]
    return report

