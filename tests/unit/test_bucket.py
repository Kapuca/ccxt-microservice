from ccxt_microservice.bucket import Bucket
import pytest


@pytest.fixture
def the_bucket():
    return Bucket(10, 1, 5)


def test_state(the_bucket):
    assert 4.99 < the_bucket.state() < 5


def test_push(the_bucket):
    the_bucket.push(5)
    assert 9.99 < the_bucket.state() < 10

def test_timeToWait(the_bucket):
    assert 4.99 < the_bucket.timeToWait(10) < 5


def test_add():
    pass


def test_wait():
    pass
