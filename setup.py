import os
from setuptools import setup, find_packages

PACKAGE_NAME = "workflow"


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name=PACKAGE_NAME,
    version="21.11.1",
    author="QuakeCoRE Software team",
    author_email="",
    description=("Install the workflow and modules"),
    license="MIT",
    keywords="UC GM Sim workflow installation",
    url="https://github.com/ucgmsim/slurm_gm_workflow",
    #        packages = ['workflow'],
    # packages=[
    #    "workflow",
    #    "workflow.automation",
    #    "workflow.automation.estimation",
    #    "workflow.automation.lib",
    #    "workflow.automation.lib.schedulers",
    #    "workflow.automation.metadata",
    #    "workflow.automation.submit",
    #    "workflow.calculation",
    #    "workflow.calculation.verification",
    #    "workflow.automation.install_scripts",
    #    "workflow.automation.tests",
    #    "workflow.automation.execution_scripts",
    #    "workflow.e2e_tests",
    # ],
    packages=find_packages("."),  # include=['workflow','workflow.*']),
    package_data={
        "workflow.automation": [
            "org/*/*.json",
            "org/*/*.pbs",
            "org/*/*.sl",
            "templates/*.template",
        ],
        "workflow.calculation": ["gmsim_templates/*/*.yaml"],
        "workflow.examples": ["*.yaml"],
        "workflow.e2e_tests": ["*.yaml", "org/*/*.json"],
        "workflow.environments": ["*.json", "org/*/*.json"],
    },
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Topic :: Workflow",
        "License :: MIT",
    ],
)
