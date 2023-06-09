from setuptools import setup, find_packages

setup(
    name="ohnlp-backbone-xlang-python",
    version="1.0.28",
    description="Python support for OHNLP Toolkit Backbone Components",
    author="Andrew Wen",
    author_email="contact@ohnlp.org",
    packages=find_packages(),
    python_requires='>3.7',
    install_requires=[
        'py4j==0.10.9.7'
    ]
)
