# -*- coding: utf-8 -*-
import os
import argparse
import compileall
import shutil

parser = argparse.ArgumentParser()
parser.add_argument('-s', type=bool, default=False, help="是否源码发布")

args = parser.parse_args()
source_code = args.s

if source_code:
    if os.path.exists('./logs'):
        shutil.rmtree('./logs')
    os.popen("git archive -o code_`git rev-parse HEAD | cut -c 1-5`.zip HEAD")
else:
    if os.path.exists('./archive_code'):
        shutil.rmtree('./archive_code')
    os.mkdir('archive_code')

    compileall.compile_dir('.', quiet=1, maxlevels=0, force=True, legacy=True)
    all_files = [f for f in os.listdir() if os.path.isfile(f) and f.endswith('pyc')
                 and f != 'gunicorn_config.pyc']
    for i in all_files:
        os.popen(f'mv {i} ./archive_code')
    os.popen(f'cp readme.md requirements.txt gunicorn_config.py ./archive_code')
    os.makedirs('./archive_code/logs')
