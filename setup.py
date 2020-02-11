from setuptools import setup


def read_list(filepath):
    with open(filepath, "r") as f:
        return [i.strip() for i in f.readlines()]


setup(
    name="meld",
    version="0.0.1",
    url="http://github.com/swarchal/meld",
    description="collating cellprofiler data from eddie jobs",
    license="MIT",
    packages=["meld"],
    tests_require=["pytest"],
    install_requires=read_list("requirements.txt"),
)
