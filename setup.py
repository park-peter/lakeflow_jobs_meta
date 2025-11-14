"""Setup configuration for Lakeflow Jobs Meta package"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="lakeflow-jobs-meta",
    version="0.1.1",
    author="Peter Park",
    author_email="peter.park@databricks.com",
    description="Metadata-driven framework for orchestrating Databricks Lakeflow Jobs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/park-peter/lakeflow-jobs-meta",
    packages=find_packages(exclude=["tests", "tests.*", "examples", "examples.*", "docs", "docs.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
    ],
    keywords="databricks jobs orchestration metadata workflow etl data-engineering",
    python_requires=">=3.10",
    install_requires=[
        "databricks-sdk>=0.65.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.12.0",
            "delta-spark>=3.2.0",
            "pyspark>=3.5.0",
            "pyyaml>=6.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "lakeflow-jobs-meta=lakeflow_jobs_meta.main:main",
        ],
    },
    include_package_data=True,
    project_urls={
        "Documentation": "https://github.com/park-peter/lakeflow-jobs-meta/blob/main/README.md",
        "Source": "https://github.com/park-peter/lakeflow-jobs-meta",
        "Tracker": "https://github.com/park-peter/lakeflow-jobs-meta/issues",
    },
)
