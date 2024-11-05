PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#
#docker run \
#    --rm \
#    --workdir='/src' \
#    -v "${PROJECT_DIRPATH}:/src/" \
#    tobix/pywine bash -c "wine pip install -r requirements.txt;
#                          wine pip install pyinstaller ;
#                          wine pyinstaller export_to_plgr1.spec \
#                               --clean \
#                               --distpath=dist/windows/;
#                          chown -R ${UID} dist; "


docker run \
    --rm \
    --workdir='/usr/src/myapp' \
    -v "${PROJECT_DIRPATH}:/usr/src/myapp" \
    python:3.10-bullseye bash -c "pip install -r requirements.txt;
                               pip3 install pyinstaller;
                               pyinstaller script/export_to_plgr_with_orders.py \
                               --clean \
                               --name export_to_plgr_with_orders \
                               --distpath=dist/linux/ \
                               --onefile -y;
                               chown -R ${UID} dist; "
