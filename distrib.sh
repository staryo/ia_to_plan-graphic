PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/src' \
    -v "${PROJECT_DIRPATH}:/src/" \
    tobix/pywine bash -c "wine pip install -r requirements.txt;
                          wine pip install pyinstaller ;
                          wine pyinstaller export_to_plgr1.spec \
                               --clean \
                               --distpath=dist/windows/;
                          chown -R ${UID} dist; "


docker run \
    --rm \
    --workdir='/usr/src/myapp' \
    -v "${PROJECT_DIRPATH}:/usr/src/myapp" \
    python:3.8 bash -c "pip install -r requirements.txt;
                               pip3 install pyinstaller;
                               pyinstaller script/export_to_plgr.py \
                               --clean \
                               --name export_to_plgr \
                               --distpath=dist/linux/ \
                               --onefile -y;
                               chown -R ${UID} dist; "
