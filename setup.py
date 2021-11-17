import os
from setuptools import setup

PACKAGE_NAME = "workflow"

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
        name= PACKAGE_NAME,
        version = "21.11.1",
        author = "QuakeCoRE Software team",
        author_email = "",
        description = ("Install the workflow and modules"),
        license = "MIT",
        keywords = "UC GM Sim workflow installation",
        url = "https://github.com/ucgmsim/slurm_gm_workflow",
        packages = ['workflow','workflow.automation','workflow.automation.estimation','workflow.automation.lib','workflow.automation.lib.schedulers','workflow.automation.metadata','workflow.automation.submit','workflow.calculation','workflow.calculation.verification'],
        long_description=read('README.md'),
        classifiers=[
            "Development Status :: 1 - Alpha",
            "Topic :: Workflow",
            "License :: MIT",
        ],
)

