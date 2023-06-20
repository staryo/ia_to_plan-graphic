import csv
import datetime


def get_raport_from_csv(csv_file):
    try:
        with open(
                f"{str(datetime.date.today())}.csv.bak",
                'r'
        ) as all_positions:
            data = csv.DictReader(all_positions)
            report = {row['CODE']: 0 for row in data}
    except FileNotFoundError:
        report = {}
    with open(csv_file, 'r') as input_file:
        data = csv.DictReader(input_file)
        for row in data:
            report[row['CODE']] = row['QUANTITY']

    report_to_write = [{
        'CODE': key,
        'QUANTITY': value
    } for key, value in report.items()]

    keys = report_to_write[0].keys()

    with open(
            f"{str(datetime.date.today())}.csv.bak",
            mode='w',
            newline=''
    ) as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(report_to_write)

    return report_to_write

