#!/usr/bin/env python

from __future__ import absolute_import, division, print_function

import os
import sys
from fnmatch import fnmatchcase

from distutils.core import Command, setup
from distutils.util import convert_path

import glob
import subprocess
import shutil

#------------------------------------------------------------------------
# Top Level Packages
#------------------------------------------------------------------------

def find_packages(where='.', exclude=()):
    out = []
    stack = [(convert_path(where), '')]
    while stack:
        where, prefix = stack.pop(0)
        for name in os.listdir(where):
            fn = os.path.join(where,name)
            if ('.' not in name and os.path.isdir(fn) and
                os.path.isfile(os.path.join(fn, '__init__.py'))
            ):
                out.append(prefix+name)
                stack.append((fn, prefix+name+'.'))

    if sys.version_info[0] == 3:
        exclude = exclude + ('*py2only*', )

    for pat in list(exclude) + ['ez_setup', 'distribute_setup']:
        out = [item for item in out if not fnmatchcase(item, pat)]

    return out

packages = find_packages()

if sys.platform == 'win32':
    dir_sep = '\\'
else:
    dir_sep = '/'


def get_data_files():
    data_files = []

    root = os.path.join("nutchpy","ex_data")

    ##scan catalog for files with the above extensions and add to pkg_data_dirs
    for path, dirs, files in os.walk(root):
        for fs in files:

            #remove nutchpy from path name
            install_path = dir_sep.join(path.split(dir_sep)[1:])
            data_files.append(os.path.join(install_path,fs))

    return data_files


package_data = dict(nutchpy=get_data_files())


#------------------------------------------------------------------------
# Commands
#------------------------------------------------------------------------

class CleanCommand(Command):
    """Custom distutils command to clean the .so and .pyc files."""

    user_options = []

    def initialize_options(self):
        self._clean_me = []
        self._clean_trees = []

        for toplevel in packages:
            for root, dirs, files in list(os.walk(toplevel)):
                for f in files:
                    if os.path.splitext(f)[-1] in ('.pyc', '.so', '.o', '.pyd', '.jar'):
                        self._clean_me.append(os.path.join(root, f))

        for d in ('build',):
            if os.path.exists(d):
                self._clean_trees.append(d)

    def finalize_options(self):
        pass

    def run(self):
        for clean_me in self._clean_me:
            try:
                print('flushing', clean_me)
                os.unlink(clean_me)
            except Exception:
                pass
        for clean_tree in self._clean_trees:
            try:
                print('flushing', clean_tree)
                shutil.rmtree(clean_tree)
            except Exception:
                pass

#------------------------------------------------------------------------
# Setup
#------------------------------------------------------------------------

longdesc = open('README.md').read()

#------------------------------------------------------------------------
# Optional building with MAVEN
#------------------------------------------------------------------------

if not 'nojava' in sys.argv:
    JAVA_SRC = "seqreader-app"
    os.chdir(JAVA_SRC)
    build_cmd = "mvn package"
    os.system(build_cmd)
    # subprocess.check_call(build_cmd, shell=os.name != 'nt',
    #                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    os.chdir("..")
    jar_file = os.path.join(JAVA_SRC,"target",
                            "seqreader-app-1.0-SNAPSHOT-jar-with-dependencies.jar")
    shutil.copy(jar_file,os.path.join("nutchpy","java_libs"))
else:
    assert 'nojava' == sys.argv.pop(2)

jar_file_list = glob.glob("nutchpy/java_libs/*")
jar_file_list = [os.path.relpath(path,start='nutchpy') for path in jar_file_list]

package_data['nutchpy'] = package_data['nutchpy']+jar_file_list

setup(
    name='nutchpy',
    version='0.1',
    author='Continuum Analytics',
    author_email='blaze-dev@continuum.io',
    description='nutchpy',
    long_description=longdesc,
    license='BSD',
    platforms = ['any'],
    install_requires=['py4j>=0.8.2.1'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Education',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Big Data',
        'Topic :: Java',
    ],
    packages=packages,
    package_data=package_data,
    cmdclass = {
        'clean': CleanCommand,
    }
)


