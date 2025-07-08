from setuptools import setup, find_packages

setup(
    name='IPOS_Sync',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'requests',
        'python-dotenv',
        'pyodbc'
    ],
    entry_points={
        'console_scripts': [
            'ipos-artemis-etl=run:main'
        ]
    }
)