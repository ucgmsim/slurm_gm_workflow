import os

from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup

PACKAGE_NAME = "workflow"


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


extensions = [Extension("merge_ts.merge_ts_loop", ["merge_ts/merge_ts_loop.pyx"])]

setup(
    name=PACKAGE_NAME,
    version="21.11.1",
    author="QuakeCoRE Software team",
    author_email="",
    description=("Install the workflow and modules"),
    license="MIT",
    keywords="UC GM Sim workflow installation",
    url="https://github.com/ucgmsim/slurm_gm_workflow",
    packages=find_packages("."),
    package_data={
        "workflow.automation": [
            "org/*/*",
            "templates/*",
            "install_scripts/*.sql",
        ],
        "workflow.calculation": ["gmsim_templates/*/*.yaml"],
        "workflow": ["*/*.yaml", "*/org/*/*.json", "*/*.json"],
    },
    entry_points={"console_scripts": ["merge_ts=merge_ts.merge_ts:main"]},
    ext_modules=cythonize(extensions),
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Topic :: Workflow",
        "License :: MIT",
    ],
)
