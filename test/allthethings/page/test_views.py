from flask import url_for

def test_home_page(client):
    """Home page should respond with a success 200."""
    response = client.get(url_for("page.home_page"))

    assert response.status_code == 200
