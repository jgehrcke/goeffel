import re
from setuptools import setup


version = re.search(
    r'^__version__\s*=\s*"(.*)"',
    open('goeffel/goeffel.py').read(),
    re.M
    ).group(1)


with open('README.md', 'rb') as f:
    long_descr = f.read().decode('utf-8')


setup(
    name='goeffel',
    packages=['goeffel'],
    entry_points={
        'console_scripts': [
            'goeffel = goeffel.goeffel:main',
            'goeffel-analysis = goeffel.analysis:main'
            ]
        },
    version=version,
    description='Measures the resource utilization of a specific process over time',
    long_description=long_descr,
    long_description_content_type='text/markdown',
    author='Dr. Jan-Philip Gehrcke',
    author_email='jgehrcke@googlemail.com',
    url='https://github.com/jgehrcke/goeffel',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Operating System :: POSIX',
        ],
    # TODO(JP):pandas, matplotlib are for the analysis program only. Make it so
    # that the measurement program can be pip-installed w/o requiring pandas and
    # matplotlib. Use https://setuptools.readthedocs.io/en/latest/setuptools.html
    # #declaring-extras-optional-features-with-their-own-dependencies
    install_requires=('tables', 'psutil', 'pandas', 'matplotlib'),
    )
