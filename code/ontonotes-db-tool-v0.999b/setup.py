#!/usr/bin/env python

from distutils.core import setup

setup(name='ontonotes-db-tool-0.999b',
      version='0.999b',
      description='OntoNotes DB Tool',
      author='Sameer Pradhan and Jeff Kaufman',
      author_email='pradhan@bbn.com',
      packages=['on', 'on.corpora', 'on.common', 'on.tools'],
      package_dir={'on': 'src/on',
                   'on.corpora': 'src/on/corpora',
                   'on.common': 'src/on/common',
                   'on.tools': 'src/on/tools'}
     )



