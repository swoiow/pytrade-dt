import datetime as dt
import re
from pathlib import Path

from setuptools import find_namespace_packages, setup


MODULE_NS = "pytrade"
BASE_DIR = Path(__file__).resolve().parent
VERSION_FILE = BASE_DIR / MODULE_NS / "ext" / "__init__.py"


def read_version() -> str:
    """Read version from file using regex to avoid code execution.

    Complies with PEP 440 and handles development/post-release tags.
    """

    default_ver = dt.datetime.now().strftime("%Y.%m.%d.%H%M%S")
    if not VERSION_FILE.exists():
        return default_ver

    content = VERSION_FILE.read_text(encoding="utf-8")
    # 兼容元组 (0, 1, 0) 或 字符串 "0.1.0"
    match = re.search(r"__VERSION__\s*=\s*[\(\"']([^\"'\)]+)[\"'\)]", content)

    if not match:
        return default_ver

    raw_version = match.group(1)
    # 处理元组情况: (0, 1, 0) -> 0.1.0
    if "," in raw_version:
        return ".".join(v.strip() for v in raw_version.split(",") if v.strip())

    return raw_version.strip()


setup(
    name="pytrade-dt",
    version=read_version(),
    description="",
    author="HarmonSir",
    author_email="git@pylab.me",
    url="https://intro-pytrade.pylab.me/",
    packages=find_namespace_packages(include=[f"{MODULE_NS}.*"]),  # 显式包含顶级命名空间
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
