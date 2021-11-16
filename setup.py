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
        packages = ['automation','automation.estimation','automation.lib','automation.lib.schedulers','automation.metadata','automation.submit','calculation','calculation.verification'],
        long_description=read('README.md'),
        classifiers=[
            "Development Status :: 1 - Alpha",
            "Topic :: Workflow",
            "License :: MIT",
        ],
)

