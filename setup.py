"""Setup configuration for Lakeflow Job Meta package"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="lakeflow-job-meta",
    version="0.1.0",
    author="Your Name",
    description="Metadata-driven framework for orchestrating Databricks Lakeflow Jobs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/lakeflow-job-meta",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "databricks-sdk>=0.1.0",
        "pyspark>=3.0.0",
        "pyyaml>=6.0",
        "delta-spark>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "lakeflow-job-meta=lakeflow_job_meta.main:main",
        ],
    },
    include_package_data=True,
)

