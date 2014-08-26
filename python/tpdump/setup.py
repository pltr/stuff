from setuptools import setup, find_packages
setup(
    name = "mysql_tpdump",
    version = "0.1",
    packages = find_packages(),
    install_requires = ['MySQL-python'],
    package_data={},
    entry_points = {
        'console_scripts': ['mysql_tpdump = mysql_tpdump:main']
    }
)