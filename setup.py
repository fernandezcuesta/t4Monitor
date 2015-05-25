#!/usr/bin/env python
from setuptools import setup

requires = ['cairocffi >= 0.6', 'Jinja2 >= 2.7.3', 'matplotlib >= 1.4.3', 'numpy >= 1.9.2',
            'pandas == 0.15.2', 'paramiko == 1.15.2', 'python-dateutil', 'pyzmq', 'six',
            'pytz >= 0a', 'cffi', 'ecdsa', 'MarkupSafe', 'mock', 'pycparser', 'pyparsing']

entry_points = {
    'console_scripts': [
        'smscmon = pysmscmon:argument_parse',
        'smscmon-config = pysmscmon:dump_config'
    ]
}


README = open('README.md').read()
CHANGELOG = open('docs/changelog.md').read()


setup(
    name="pysmscmon",
    version="0.6.3",
    url='https://github.com/fernandezcuesta/pySMSCMon',
    author='JM Fernandez',
    author_email='fernandez.cuesta@gmail.com',
    description="A tool to monitor Acision SMSC without AMS_PMS",
    long_description=README + '\n' + CHANGELOG,
    packages=['pysmscmon', 'pysmscmon.sshtunnels'],
    include_package_data=True,
    install_requires=requires,
    entry_points=entry_points,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
         'Environment :: Console',
         'License :: OSI Approved :: GNU Affero General Public License v3',
         'Operating System :: OS Independent',
         'Programming Language :: Python :: 2',
         'Programming Language :: Python :: 2.7',
         'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    test_suite='pysmscmon.tests',
)
