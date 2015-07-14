#!/usr/bin/env python
from setuptools import setup
from setuptools.command.test import test as TestCommand
from pip.req import parse_requirements
from pip.download import PipSession

import sys
import pysmscmon


requires = [str(ir.req) for ir in parse_requirements('requirements.txt', session=PipSession)]

entry_points = {
    'console_scripts': [
        'smscmon = pysmscmon:argument_parse',
        'smscmon-config = pysmscmon:dump_config'
    ]
}


README = open('README.md').read()
CHANGELOG = open('changelog.md').read()

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
    name="pysmscmon",
    version=pysmscmon.__version__,
    url='https://github.com/fernandezcuesta/pySMSCMon',
    license='MIT license',
    author='JM Fernandez',
    author_email='fernandez.cuesta@gmail.com',
    description="A tool to monitor Acision SMSC without AMS_PMS",
    long_description=README + '\n' + CHANGELOG,
    packages=['pysmscmon', 'pysmscmon.sshtunnels'],
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
         'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    tests_require=['tox'],
    cmdclass = {'test': Tox},
    test_suite='pysmscmon.tests.test_pysmscmon',
)
