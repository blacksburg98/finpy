from setuptools import setup

setup(
    name='FinPy',
    version='0.1.006',
    author='Tsung-Han Yang',
    author_email='blacksburg98@yahoo.com',
    packages=['finpy'],
    package_data={'finpy': ['finpy_data/Yahoo/*.csv', '*.txt',
        'finpy_data/Yahoo/*.txt', 'finpy_data/Yahoo/Lists/*']},
    scripts=['scripts/marketsim.py'],
    url='http://pypi.python.org/pypi/FinPy/',
    license='LICENSE.txt',
    description='Financial Python. Using python to do stock analysis.',
    long_description=open('README.txt').read(),
    install_requires=[
        "NumPy >= 1.6.1",
        "pandas >= 0.7.3",
        "matplotlib >= 1.2.1",
        "python == 2.7",
    ],
    classifiers = [
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "Programming Language :: Python :: 2.7"
    ],
)

