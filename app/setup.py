from setuptools import find_packages, setup

with open("README.md","r") as f:
    long_description = f.read()

setup(
    name = "crash_accidents_analysis",
    version = "0.1",
    description ="Analyses crash data to provide answers to given questions",
    packages_dir = {"":"src"},
    packages=find_packages(where = "src"),
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "",
    author = "Avaneesh Pareek",
    author_email = "pareekavi10@gmail.com",
    license = "None",
    classifiers = [
        "License :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Operating System :: MacOS"
    ],
    install_requires = ["pyspark>=3.5.3"],
    extras_require = {"dev":["twine>=4.0.2"]},
    python_requires = ">=3.10"
)