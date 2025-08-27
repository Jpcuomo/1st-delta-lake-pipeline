from setuptools import setup, find_packages

setup(
    name="delta_lake_bucket",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.28.0",
        "pandas>=1.5.0",
        "deltalake>=0.10.0",
        "ydata-profiling>=4.0.0",
    ]
)