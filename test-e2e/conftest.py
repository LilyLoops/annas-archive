from typing import Generator
import re
import pytest
from playwright.sync_api import Page, Playwright, APIRequestContext, expect


@pytest.fixture(scope="session")
def api_request_context(playwright: Playwright) -> Generator[APIRequestContext, None, None]:
    request_context = playwright.request.new_context(base_url="http://localtest.me:8000")
    yield request_context
    request_context.dispose()


@pytest.fixture(scope="session")
def account_id(api_request_context: APIRequestContext) -> Generator[str, None, None]:
    account = api_request_context.post("/account/register")
    assert account.ok
    body = account.text()
    needle = ">/account/?key="
    start = body.index(needle)
    end = body.index('<', start)
    secret_key = body[start + len(needle):end]
    yield secret_key


@pytest.fixture()
def authenticated_page(account_id: str, page: Page) -> Generator[Page, None, None]:
    page.goto("http://localtest.me:8000/account")
    page.get_by_role("textbox", name="Secret key").fill(account_id)
    page.get_by_role("button", name="Log in").click()
    # bypass the flask-debug page
    expect(page).to_have_url(re.compile(r"/account/$"))
    page.get_by_role("link").click()
    expect(page.get_by_text("Membership: None")).to_be_visible()
    page.goto("http://localtest.me:8000/")
    yield page
