import pytest


@pytest.fixture
def clean_hooks():
    """Keep Hook registrations isolated for tests that opt into this fixture."""
    from memos.plugins.hooks import _hooks

    _hooks.clear()
    yield
    _hooks.clear()
