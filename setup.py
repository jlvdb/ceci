from setuptools import setup

<<<<<<< HEAD
setup()
=======
# read the contents of the README file
with open('README.md', encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='ceci',
    description='Lightweight pipeline engine for LSST DESC',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/LSSTDESC/ceci',
    maintainer='Joe Zuntz',
    license='BSD',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 3 - Alpha',
    ],
    packages=['ceci', 'ceci.sites', 'ceci.provenance'],
    entry_points={
        'console_scripts':['ceci=ceci.main:main']
    },
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    install_requires=['pyyaml', 'psutil', 'dict-io'],
    extras_require={
      'parsl': ['flask', 'parsl>=1.0.0'],
      'cwl': ['cwlgen>=0.4', 'cwltool>=2.0.20200126090152'],
      'test': ['pytest', 'codecov', 'pytest-cov', 'fitsio', 'h5py', "ruamel.yaml"],
      }
)
>>>>>>> 2806b1d (add more test reqs)
