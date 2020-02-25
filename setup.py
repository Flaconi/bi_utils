from setuptools import setup

setup(
    name='utils',
    version='0.0.1',
    description='common utils library shared between DE and DS teams to avoid duplication and maintenance efforts',
    url='https://github.com/Flaconi/utils.git',  #'git@github.com/Flaconi/utils.git',
    author='Anna Anisienia',
    author_email='anna.anisienia@flaconi.de',
    license='Flaconi',
    packages=['utils'],
    zip_safe=False
)

# pip install git+ssh://git@github.com/Flaconi/utils.git - didn't work
# pip install git+https://github.com/Flaconi/utils.git 