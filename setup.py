import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
        name= "workflow",
        version = "21.11.1",
        author = "QuakeCoRE Software team",
        author_email = "",
        description = ("Install the workflow and modules"),
        license = "MIT",
        keywords = "UC GM Sim workflow installation",
        url = "https://github.com/ucgmsim/slurm_gm_workflow",
        packages = ['automation','calculation','e2e_tests','environments','examples','scripts'],
        long_description=read('README.md'),
        classifiers=[
            "Development Status :: 1 - Alpha",
            "Topic :: Workflow",
            "License :: MIT",
        ],
)

