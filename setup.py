from setuptools import setup

setup(
    install_requires=[
        'path.py>=8.1.2',
        'python-keystoneclient>=1.8.1',
        'python-swiftclient>=2.6.0',
    ],
    pbr=True,
    setup_requires=['pbr'],
)
