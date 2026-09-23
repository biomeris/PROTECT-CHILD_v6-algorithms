from os import path
from codecs import open
from setuptools import setup, find_packages

# we're using a README.md, if you do not have this in your folder, simply
# replace this with a string.
here = path.abspath(path.dirname(__file__))
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Here you specify the meta-data of your package. The `name` argument is
# needed in some other steps.
setup(
    name="v6-mann-whitney-u-test-py",
    version="1.0.0",
    description="Federated Mann-Whitney U test for non-parametric comparison of two independent groups across multiple organizations without sharing raw data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/biomeris/PROTECT-CHILD_v6-algorithms/tree/main/v6-mann-whitney-u-test-py",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=["vantage6-algorithm-tools==4.9.1", "pandas", "numpy", "scipy"],
)
