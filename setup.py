from setuptools import setup

version = "0.1.dev0"

long_description = "\n\n".join([open("README.rst").read(), open("CHANGES.rst").read()])

install_requires = []

tests_require = ["pytest", "mock", "pytest-cov", "pytest-flakes", "pytest-black"]

setup(
    name="spiceup-labels",
    version=version,
    description="calculate labels for spiceup apps from labeltypes based on dask-geomodeling and lizard data (rasters, parcels, labelparameters)",
    long_description=long_description,
    # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
    classifiers=["Programming Language :: Python", "Framework :: Django"],
    keywords=[],
    author="wietze suijker",
    author_email="wietze.suijker@nelen-schuurmans.nl",
    url="https://github.com/nens/spiceup-labels",
    license="MIT",
    packages=["spiceup_labels"],
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={"test": tests_require},
    entry_points={
        "console_scripts": [
            "run-spiceup-labels = spiceup_labels.patch_calendar_tasks:main",
            "run-spiceup-labels = spiceup_labels.patch_warning_based_tasks:main",
        ]
    },
)
