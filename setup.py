from setuptools import setup, find_packages

setup(
    name="pytrade-dt",
    version="0.0.1",
    description="",
    author="HarmonSir",
    author_email="git@pylab.me",
    url="<your_project_url>",
    packages=find_packages(include=["pytrade", "pytrade.*"]),
    install_requires=[
        "requests>=2.0.0",
        "pandas>=2.0.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12",
)
