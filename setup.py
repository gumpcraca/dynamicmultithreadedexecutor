#!/usr/bin/env python

from setuptools import setup, find_packages

print("STARTING INSTALL!!!!")
print(find_packages())

setup(name='dynamicmultithreadedexecutor',
      version='1.0.2',
      description='Dynamic Multi-threaded Executor',
      author='Kevin McCabe',
      author_email='csmp@hotmail.com',
      url='https://github.com/gumpcraca/dynamicmultithreadedexecutor',
      download_url="https://github.com/gumpcraca/dynamicmultithreadedexecutor/archive/1.0.2.zip",
      keywords = [],
      packages=find_packages(),
      install_requires=['six','sentinels'],
      py_modules=["six"],
      classifiers = [],
     )
