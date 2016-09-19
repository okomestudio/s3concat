# -*- coding: utf-8 -*-
import pytest
from moto import mock_s3


def pytest_addoption(parser):
    parser.addoption(
        '--use-s3', action='store_true',
        help='Run tests against S3, not mock')


@pytest.fixture(scope='session')
def s3(request):
    use_s3 = pytest.config.getoption('use_s3')

    m = None
    if not use_s3:
        m = mock_s3()
        m.start()

    from s3concat import resources
    yield resources.s3

    if m:
        m.stop()
