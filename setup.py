from setuptools import setup

setup(
    name='bi_utils',
    version='0.0.9',
    description='common utility library shared between DE and DS teams to avoid duplication and maintenance efforts',
    url='http://github.com/Flaconi/bi_utils.git', 
    author='Ben Bauer & Anna Anisienia',
    author_email='ben.bauer@flaconi.de',
    license='Flaconi',
    packages=['bi_utils'],
    install_requires=["python-dotenv>=0.12.0"],
    zip_safe=False
)

# pip install git+https://github.com/Flaconi/bi_utils.git 
