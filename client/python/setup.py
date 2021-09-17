import setuptools

setuptools.setup(
    name="TalariaClient",
    version="0.0.6",
    author="Chun Rong Phang",
    author_email="crphang@gmail.com",
    description="Talaria Client to ingest events to TalariaDB",
    url="https://github.com/kelindar/talaria",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "grpcio>=1.36.0",
        "protobuf~=3.17.3"
    ],
    python_requires='>=3.6',
)
