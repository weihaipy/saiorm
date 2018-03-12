import io
import os

from setuptools import setup, find_packages

with io.open(os.path.join("docs", 'index.rst'), encoding='utf-8') as f:
    readme = f.read()

with io.open('requirements.txt') as f:
    requirements = f.readlines()

version = "0.0.2"

setup(
    name="saiorm",
    version=version,
    url='https://github.com/weihaipy/saiorm',
    author='James',
    author_email='xxx_828@126.com',
    maintainer='James',
    maintainer_email='xxx_828@126.com',
    description='Saiorm is a simple ORM.',
    long_description=readme,
    license="MIT",
    packages=find_packages(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    install_requires=requirements,
)
