from setuptools import setup


with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    name="ohnlp-backbone-xlang-python",
    version="1.0.0",
    description="Python support for OHNLP Toolkit Backbone Components",
    author="Andrew Wen",
    author_email="contact@ohnlp.org",
    packages=['ohnlp'],
    install_requires=requirements
)
