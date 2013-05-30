import os
from setuptools import setup, find_packages
import backdrop

requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
requires = map(str.strip, open(requirements_path).readlines())

readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
long_description = open(readme_path).read()

setup(
    name=backdrop.__title__,
    version=backdrop.__version__,
    description='The backend applications for the Performance Platform',
    long_description=long_description,
    author=backdrop.__author__,
    author_email='none@nowhere',
    url='https://github.com/alphagov/backdrop',
    packages=find_packages(exclude=['tests*', 'features*']),
    include_package_data=True,
    install_requires=requires,
    license='https://github.com/alphagov/backdrop/master/LICENCE.txt',
)
