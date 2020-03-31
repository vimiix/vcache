#!/usr/bin/env python
# coding=utf-8

from os import path as os_path
from setuptools import setup

this = os_path.abspath(os_path.dirname(__file__))


def read_file(filename):
    with open(os_path.join(this, filename), encoding="utf-8") as f:
        long_description = f.read()
    return long_description


setup(
    name="vcache",
    version="0.0.7",
    author="vimiix",
    author_email="vimiix.py@gmail.com",
    url="https://github.com/vimiix/vcache",
    description="Python implementation of go-redis/cache",
    long_description=read_file("README.md"),
    long_description_content_type="text/markdown",
    packages=["vcache"],
    install_requires=["cacheout"],
    license="MIT",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=["cache", "localcache"],
)
