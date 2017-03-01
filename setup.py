#!/usr/bin/env python
import sys

import versioneer

from pip.req import parse_requirements
from setuptools import setup
from pip.download import PipSession
from setuptools.command.test import test as TestCommand

install_requires = [str(ir.req) for ir in
                    parse_requirements('requirements/requirements.txt',
                                       session=PipSession)]
tests_require = [str(ir.req) for ir in
                 parse_requirements('requirements/requirements-test.txt',
                                    session=PipSession)]

if sys.platform.startswith('linux') or sys.platform == 'darwin':
    install_requires.append('cairocffi')

entry_points = {
    'console_scripts': [
        't4monitor = t4mon:main',
    ]
}

README = open('README.rst').read()
CHANGELOG = open('changelog.rst').read()


class Tox(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['--recreate']
        self.test_suite = True

    def run_tests(self):
        # import here, otherwise eggs aren't loaded
        import tox
        errcode = tox.cmdline(self.test_args)
        sys.exit(errcode)

cmdclass_ = versioneer.get_cmdclass()
cmdclass_['test'] = Tox

setup(
    name="t4Monitor",
    version=versioneer.get_version(),
    cmdclass=cmdclass_,
    url='https://github.com/fernandezcuesta/t4Monitor',
    license='MIT',
    author='JM Fernandez',
    author_email='fernandez.cuesta@gmail.com',
    description="Report OpenVMS hosts from T4 statistics",
    long_description=README + '\n' + CHANGELOG,
    packages=['t4mon', 'sshtunnels'],
    include_package_data=True,
    install_requires=install_requires,
    tests_require=tests_require,
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
)
