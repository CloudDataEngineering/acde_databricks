import setuptools

setuptools.setup(
    name="mmmingest",
    version="0.0.1",
    description="MMM Ingest is a PySpark and Databricks Framework that contains    data processing libraries and has the ablity to ingest different data sources across the multiple data ingestion layers fro different applications",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3"
        "Open Source :: Linux",
    ],
    install_requires=[
        "openpyxl==3.1.2",
    ],
)