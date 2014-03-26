from distutils.core import setup

setup(
    name='lazytable',
    version='0.2',
    packages=['lazytable',],
    url='https://github.com/samuelinsf/lazytable',
    license='Software license AGPL version 3.',
    description='A basic sqlite table wrapper for python',
    long_description=read('README.rst'),
    author='samuelinsf',
)

#to upload: python setup.py sdist upload
