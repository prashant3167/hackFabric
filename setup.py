from setuptools import setup, find_packages

setup(
    name="Fabric",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"test": "src"},
)
