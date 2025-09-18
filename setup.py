import os
import sys

from setuptools import find_packages, setup

# Add src directory to path so we can import the version
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from propel_data_platform._version import __version__

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [
        line.strip() for line in fh if line.strip() and not line.startswith("#")
    ]

with open("requirements-dev.txt", "r", encoding="utf-8") as fh:
    dev_requirements = [
        line.strip() for line in fh if line.strip() and not line.startswith("#")
    ]

setup(
    name="propel-data-platform",
    version=__version__,
    author="Mayank Shinde",  # Replace with your name
    author_email="mayank@propel.bz",  # Replace with your email
    description="A package for managing data connectors",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/propel-bz/data-platform",  # Replace with your repository URL
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
    },
    entry_points={
        "console_scripts": [
            "execute_task=propel_data_platform.tasks:execute_task"
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
