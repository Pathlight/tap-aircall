from setuptools import setup

setup(name='tap-aircall',
      version='0.1.5',
      description='Singer.io tap for extracting data from the Aircall API',
      author='Pathlight',
      url='https://www.pathlight.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_aircall'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.21.0',
          'singer-python==5.8.0'
      ],
      entry_points='''
          [console_scripts]
          tap-aircall=tap_aircall:main
      ''',
      packages=['tap_aircall']
)
