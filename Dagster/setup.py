from setuptools import find_packages, setup

setup(
    name="Dagster",
    packages=find_packages(exclude=["Dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
