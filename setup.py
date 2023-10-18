import os
import setuptools
import psr.graf


with open("README.md", "r") as fh:
    long_description = fh.read()


native_module = setuptools.Extension(
    name="psr.graf._grafc",
    sources=["python_c_extension/graf.cpp"],
)


setuptools.setup(
    name="psr-graf",
    version=psr.graf.version(),
    author="PSR",
    ext_modules=[native_module],
    description="Utility module to read PSR Sddp's hdr/bin result file pairs.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/psrenergy/pygraf",
    packages=setuptools.find_namespace_packages(include=['psr.*']),
    py_modules=["psr.graf"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6'
    )
