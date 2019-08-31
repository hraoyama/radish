from setuptools import find_packages, setup

setup(name='radish',
      version='0.1',
      description='Backtest platform',
      url='',
      author='XHF',
      author_email='',
      license=None,
      packages=find_packages(include=['radish', 'radish.*']),
      install_requires=[
          'absl-py',
          'arctic',
          'bidict',
          'blosc',
          'python-socketio',
          'pyyaml',
          'ratelimit',
          'redis',
          'retry',
          'slackclient',
          'pandas',
          'numpy',
          'matplotlib'
      ],
      zip_safe=True)
