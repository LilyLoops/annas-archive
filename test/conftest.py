import os
import pytest


@pytest.fixture(scope="session")
def app():
    """
    Setup our flask test app, this only gets executed once.

    :return: Flask app
    """
    params = {
        "DEBUG": False,
        "TESTING": True,
        "WTF_CSRF_ENABLED": False,
        "DATA_IMPORTS_MODE": "1",
        "SERVER_NAME": "localhost:8000",
        "PREFERRED_URL_SCHEME": "http",
    }

    os.environ['SECRET_KEY'] = "a_very_insecure_key_for_test_padded"
    os.environ['DOWNLOADS_SECRET_KEY'] = "a_very_insecure_key_for_test_padded"
    os.environ['AA_EMAIL'] = "aa@example.com"

    # import *after* setting the constants
    from allthethings.app import create_app
    _app = create_app(settings_override=params)

    # Establish an application context before running the tests.
    ctx = _app.app_context()
    ctx.push()

    yield _app

    ctx.pop()


@pytest.fixture(scope="function")
def client(app):
    """
    Setup an app client, this gets executed for each test function.

    :param app: Pytest fixture
    :return: Flask app client
    """
    yield app.test_client()



@pytest.fixture(scope="function")
def session():
    """
    Allow very fast tests by using rollbacks and nested sessions. This does
    require that your database supports SQL savepoints, and Postgres does.

    Read more about this at:
    http://stackoverflow.com/a/26624146

    :param db: Pytest fixture
    :return: None
    """
    pass
