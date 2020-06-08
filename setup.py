from setuptools import setup

setup(
    name='bi_utils',
    version='0.0.3',
    description='common utility library shared between DE and DS teams to avoid duplication and maintenance efforts',
    url='http://github.com/Flaconi/bi_utils.git', 
    author='Anna Anisienia',
    author_email='anna.anisienia@flaconi.de',
    license='Flaconi',
    packages=['bi_utils'],
    install_requires=["python-dotenv>=0.12.0", "yaml>==5.1.1"],
    zip_safe=False
)

# pip install git+https://github.com/Flaconi/bi_utils.git 
