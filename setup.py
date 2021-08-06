# -*- coding: utf-8 -*-
import re
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile


all_deps = [
    'bitarray-2.0.1',
    'MarkupSafe',
    'Werkzeug',
    'Jinja2',
    'click',
    'pytz',
    'six',
    'ply',
    'python_dateutil',
    'numpy',
    'pandas',
    'itsdangerous',
    'Flask',
    'thriftpy2-0.4.14',
    'thrift',
    'pure_sasl',
    'thrift_sasl',
    'impyla',
    'certifi-2020.12.5',
    'cx_Oracle-8.1.0',
    'gunicorn-20.1.0',
    'pybloom-mirror-2.0.0',
    'PyMySQL-1.0.2',
    'setuptools-57.0.0',
    'GBaseConnector-1.0.0',
    'psycopg2_binary'
]

root = os.path.dirname(os.path.abspath(__file__))


def unpack_and_install(package_name):
    packed_file = package_name + '.tar.gz'
    archive = os.path.join(root, 'pkgs', 'source_code', packed_file)
    tmpd = tempfile.mkdtemp()
    failed = None
    print(f'  Unpack: {packed_file} in {tmpd}')
    try:
        with tarfile.open(archive, mode='r:gz') as tf:
            tf.extractall(tmpd)
        package_instdir = os.path.join(tmpd, package_name)
        print(f'  Installing from {package_instdir}')
        cmd = [python, 'setup.py', 'install']
        r = subprocess.call(cmd, cwd=package_instdir)
        if r != 0:
            failed = package_name
            print("Error: Installation of %s failed, code = %s" % (packed_file, r))
    finally:
        shutil.rmtree(tmpd)
        return failed


def install_wheels(wheel_name):
    if sys.platform == 'darwin':
        wheel_path = os.path.join(root, 'pkgs', 'macos')
    elif sys.platform == 'linux':
        wheel_path = os.path.join(root, 'pkgs', 'linux')
    else:
        wheel_path = os.path.join(root, 'pkgs', 'windows')
    none_platforms = os.path.join(root, 'pkgs', 'all_platforms')
    full_name1 = get_full_name(wheel_path, wheel_name)
    if full_name1 is None:
        full_name2 = get_full_name(none_platforms, wheel_name)
        if full_name2 is None:
            print(f"Error: Installation of {wheel_name} failed, 文件不存在")
            return wheel_name
        else:
            wheel_path = none_platforms
            full_name = full_name2
    else:
        full_name = full_name1
    wheel_file = os.path.join(wheel_path, full_name)
    cwd = ['pip', 'install', wheel_file]
    print(f'  Installing from {wheel_file}')
    r = subprocess.call(cwd)
    if r != 0:
        print("Error: Installation of %s failed, code = %s" % (wheel_file, r))
        return wheel_name
    else:
        return None


def get_full_name(path, pkg_name):
    for i in os.listdir(path):
        if i.startswith(pkg_name):
            return i
    return None


def install_deps(pkg):
    if re.findall(pattern, pkg):
        r = unpack_and_install(pkg)
    else:
        r = install_wheels(pkg)
    return r


if __name__ == '__main__':
    version = sys.version_info
    major, minor = version.major, version.minor
    if major != 3 or minor != 8:
        raise SystemError("不正确的python版本！")
    not_success_pkg = []
    python = sys.executable
    pattern = r'\d+\.\d+\.\d+'
    for dep in all_deps:
        print(f'>>>Installing {dep}...')
        r = install_deps(dep)
        if r:
            not_success_pkg.append(r)
    if not_success_pkg:
        print(f'\n\n以下包未成功安装：{not_success_pkg}')
    else:
        print('\n\n<<<Finish all installation!')
