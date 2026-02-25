from setuptools import setup, find_packages

setup(
    name="harbor-common",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary>=2.9",
        "flask>=3.0",
        "requests>=2.31",
        "cryptography>=42.0.0",
    ],
)
