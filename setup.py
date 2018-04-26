from distutils.core import setup

setup(
    name='lazytable',
    version='0.5.0',
    packages=['lazytable',],
    url='https://github.com/samuelinsf/lazytable',
    license='Software license AGPL version 3.',
    description='A basic sqlite table wrapper for python',
    long_description=open('README.rst').read(),
    author='samuelinsf',
)

#to upload: python3 setup.py sdist upload
