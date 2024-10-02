import json
from flask import url_for


def test_up(client):
    """Up should respond with a success 200."""
    response = client.get(url_for("dyn.index"))

    assert response.status_code == 200
    assert json.loads(response.text) == {'aa_logged_in': 0}

def test_up_databases(client):
    """Up databases should respond with a success 200."""
    response = client.get(url_for("dyn.databases"))

    assert response.status_code == 200
