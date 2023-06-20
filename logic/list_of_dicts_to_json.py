import json
from pathlib import Path


def list_of_dicts_to_json(data, path, folder):
    print('Отправка {} записей в {}{}'.format(
        len(data),
        path,
        folder
    ))
    target_folder = path + folder
    Path(target_folder).mkdir(parents=True, exist_ok=True)
    for row in data:
        with open('{}{}.json'.format(target_folder, row['identity']), 'w') as output_file:
            json.dump(row, output_file)
