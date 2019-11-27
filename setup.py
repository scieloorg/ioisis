#!/usr/bin/env python3
import ast
import os

import setuptools


setup_path = os.path.dirname(__file__)

# Get __version__ from the package __init__.py without importing it
with open(os.path.join(setup_path, "ioisis", "__init__.py")) as dinit:
    assignment_node = next(el for el in ast.parse(dinit.read()).body
                              if isinstance(el, ast.Assign) and
                                 el.targets[0].id == "__version__")
    version = ast.literal_eval(assignment_node.value)

with open(os.path.join(setup_path, "README.md")) as readme:
    long_description = readme.read()


setuptools.setup(
    name="ioisis",
    version=version,
    author="Danilo de Jesus da Silva Bellini",
    author_email="danilo.bellini@gmail.com",
    url="https://github.com/scieloorg/ioisis",
    description="I/O for ISIS files in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="2-clause BSD",
    packages=setuptools.find_packages(exclude=["tests"]),
    include_package_data=True,
    python_requires=">=3.6",
    install_requires=[
        "click",
        "construct",
        "JPype1",
        "ujson",
    ],
    entry_points={"console_scripts": ["ioisis = ioisis.__main__:main"]},
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Environment :: Other Environment",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Java",  # Because of Bruma.jar
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Operating System :: OS Independent",
    ],
)
