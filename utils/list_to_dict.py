def list_to_dict(list_data, column='id'):
    report = dict()
    for row in list_data:
        report[row[column]] = {key: value for key, value in row.items()}

    return report
