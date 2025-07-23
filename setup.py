from setuptools import setup, find_packages

setup(
    name="pyrqg",
    version="0.1.0",
    description="Python Random Query Generator",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "psycopg2-binary>=2.9.0",
    ],
    entry_points={
        "console_scripts": [
            "pyrqg=pyrqg.cli:main",
        ],
    },
)
