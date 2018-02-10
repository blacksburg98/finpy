# python setup.py bdist_wheel sdist
import os
from setuptools import setup, find_packages
__currdir__ = os.getcwd()
__readme__ = os.path.join(__currdir__, 'README.txt')
setup(
    name='FinPy',
    version='0.5',
    author='Tsung-Han Yang',
    author_email='blacksburg98@yahoo.com',
    packages=find_packages(),
    package_data={'finpy': ['data/Yahoo/*.csv', 'utils/*.txt',
        'data/Yahoo/*.txt', 'data/Yahoo/Lists/*']},
    scripts=['scripts/marketsim.py'],
    url='http://pypi.python.org/pypi/FinPy/',
    license='LICENSE.txt',
    description='Financial Python. Using python to do stock analysis.',
    long_description=open(__readme__).read(),
    install_requires=[
        "NumPy >= 1.6.1",
        "pandas >= 0.7.3",
        "dyplot >= 0.8.8"
    ],
    classifiers = [
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "Programming Language :: Python :: 3"
    ],
)

