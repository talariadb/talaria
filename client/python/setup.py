import setuptools

setuptools.setup(
    name="TalariaClient",
    version="0.0.5",
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
        "grpcio~=1.28.1",
        "protobuf~=3.11.3"
    ],
    python_requires='>=3.4',
)
