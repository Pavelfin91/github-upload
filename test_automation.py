import pytest


def shout():
    raise Exception('i am shouting')


def math(a, b):
    if a>b:
        return True
    return False


def test_aa():
    shout()


def test_math1():
    assert math(4, 2)


def test_math2():
    assert math(2, 4)
