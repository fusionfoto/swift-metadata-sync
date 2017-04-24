#!/usr/bin/python

from setuptools import setup

with open('requirements.txt') as reqs_file:
    reqs = [req.strip() for req in reqs_file]

setup(name='swift-metadata-sync',
      version='0.0.5',
      author='SwiftStack',
      test_suite='nose.collector',
      url='https://github.com/swiftstack/swift-metadata-sync',
      packages=['swift_metadata_sync'],
      install_requires = reqs,
      entry_points={
          'console_scripts': [
              'swift-metadata-sync = swift_metadata_sync.__main__:main'
          ],
      })
