import re
from setuptools import setup


version = re.search(
    r'^__version__\s*=\s*"(.*)"',
    open('schniepel/schniepel.py').read(),
    re.M
    ).group(1)


with open('README.md', 'rb') as f:
    long_descr = f.read().decode('utf-8')


setup(
    name='schniepel',
    packages=['schniepel'],
    entry_points={
        'console_scripts': [
            'schniepel = schniepel.schniepel:main',
            'schniepel-analysis = schniepel.analysis:main'
            ]
        },
    version=version,
    description='Measures the resource utilization of a specific process over time',
    long_description=long_descr,
    long_description_content_type='text/markdown',
    author='Dr. Jan-Philip Gehrcke',
    author_email='jgehrcke@googlemail.com',
    url='https://github.com/jgehrcke/schniepel',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Operating System :: POSIX',
        ],
    install_requires=('tables', 'pandas', 'psutil'),
    )
