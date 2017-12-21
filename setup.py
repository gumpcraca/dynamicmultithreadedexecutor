#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='dynamicmultithreadedexecutor',
      version='1.0',
      description='Dynamic Multi-threaded Executor',
      author='Kevin McCabe',
      author_email='csmp@hotmail.com',
      url='https://github.com/gumpcraca',
      packages=find_packages('.'),
      install_requires=['six'],
     )
