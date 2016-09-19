.. image:: https://circleci.com/gh/okomestudio/s3concat/tree/development.svg?style=shield
   :target: https://circleci.com/gh/okomestudio/s3concat/tree/development

.. image:: https://coveralls.io/repos/github/okomestudio/s3concat/badge.svg?branch=development
   :target: https://coveralls.io/github/okomestudio/s3concat?branch=development


s3concat
========

Python utility for concatinating S3 objects.

`s3concat` is licensed under the `MIT License (MIT)`_.

.. _MIT License (MIT): https://raw.githubusercontent.com/okomestudio/s3concat/development/LICENSE.txt


Basic Usage
-----------

.. code-block:: python

   from s3concat import s3concat

   s3concat(['s3://mybucket/concatenated',
             's3://mybucket/obj1',
             's3://mybucket/obj2',
             's3://myanotherbucket/obj5'])


Installation
------------

.. code-block:: bash

   pip install s3concat
