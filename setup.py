from setuptools import setup

setup(
    pbr=True,
    setup_requires=['pbr'],
    entry_points={
        'console_scripts': [
            'stor = storage_utils.cli:main'
        ]
    }
)
