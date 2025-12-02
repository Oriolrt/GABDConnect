from setuptools import setup, find_packages
import codecs
import os


here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

version="3.3.2"
DESCRIPTION = 'Classes to connect to Oracle and MongoDB server directly or through ssh tunneling.'

# Setting up
setup(
    name="GABDConnect",
    version=version,
    author="Oriol Ramos Terrades, Carles Sanchez",
    author_email="{oriol.ramos,carlos.sanchez.ramos}@uab.cat",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    package_dir={},
    packages=find_packages(exclude='test'),
    install_requires=[line.strip() for line in open("requirements.txt").readlines()],
    keywords=['python', 'databases', "oracle","mongodb"],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
