#!/usr/bin/env python
import os
import sys

from pip.req import parse_requirements
from setuptools import setup
from pip.download import PipSession
from setuptools.command.test import test as TestCommand

import t4mon

requires = [str(ir.req) for ir in parse_requirements('requirements-common.txt', session=PipSession)]

if sys.platform.startswith('linux') or sys.platform == 'darwin':
    requires.append('cairocffi')

entry_points = {
    'console_scripts': [
        't4monitor = t4mon:main'
    ]
}


README = open('README.rst').read()
CHANGELOG = open('changelog.rst').read()

class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        # import here, otherwise eggs aren't loaded
        import tox
        errcode = tox.cmdline(self.test_args)
        sys.exit(errcode)

setup(
    name="t4Monitor",
    version=t4mon.__version__,
    url='https://github.com/fernandezcuesta/t4Monitor',
    license='MIT license',
    author='JM Fernandez',
    author_email='fernandez.cuesta@gmail.com',
    description="Report OpenVMS hosts from T4 statistics",
    long_description=README + '\n' + CHANGELOG,
    packages=['t4mon', 'sshtunnels'],
    include_package_data=True,
    install_requires=requires,
    entry_points=entry_points,
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
         'Environment :: Console',
         'License :: OSI Approved :: MIT License',
         'Operating System :: OS Independent',
         'Programming Language :: Python :: 2',
         'Programming Language :: Python :: 2.7',
         'Programming Language :: Python :: 3',
         'Programming Language :: Python :: 3.3',
         'Programming Language :: Python :: 3.4',
         'Programming Language :: Python :: 3.5',
         'Topic :: System :: Monitoring',
    ],
    tests_require=['tox'],
    cmdclass = {'test': Tox},
    test_suite='t4mon.tests',
)
