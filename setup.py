from setuptools import setup

__version__ = "1.0.20220328214255"

setup(
    name="cars",
    version=__version__,
    packages=[
        "cars",
        "cars.app",
        "cars.util",
        "cars.analysis",
        "cars.scrapers",
    ],
    url="https://github.com/qdbp/cars.git",
    license="",
    author="Evgeny Naumov",
    author_email="",
    description="used car shopping tool",
)
