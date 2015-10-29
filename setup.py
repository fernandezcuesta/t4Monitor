#!/usr/bin/env python
from setuptools import setup
from setuptools.command.test import test as TestCommand
from pip.req import parse_requirements
from pip.download import PipSession

import sys
import t4mon


requires = [str(ir.req) for ir in parse_requirements('requirements.txt', session=PipSession)]

entry_points = {
    'console_scripts': [
        't4monitor = t4mon:main',
        't4mon-config = t4mon:dump_config',
        't4mon-local = t4mon:create_reports_from_local_pkl',
        't4mon-localcsv = t4mon:create_reports_from_local_csv',
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
         'Topic :: System :: Monitoring',
    ],
    tests_require=['tox'],
    cmdclass = {'test': Tox},
    test_suite='t4mon.tests',
)
