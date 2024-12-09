from typing import Generator
import pytest
from playwright.sync_api import Page, Playwright, APIRequestContext, expect
import urllib


@pytest.fixture(scope="session")
def api_request_context(playwright: Playwright) -> Generator[APIRequestContext, None, None]:
    request_context = playwright.request.new_context(base_url="http://localtest.me:8000")
    yield request_context
    request_context.dispose()


@pytest.fixture(scope="session")
def account_id(api_request_context: APIRequestContext) -> Generator[str, None, None]:
    account = api_request_context.post("/account/register")
    assert account.ok
    parsed = urllib.parse.urlparse(account.url)
    query = urllib.parse.parse_qs(parsed.query)
    secret_key = query["key"][0]
    yield secret_key


@pytest.fixture()
def authenticated_page(account_id: str, page: Page) -> Generator[Page, None, None]:
    page.goto("http://localtest.me:8000/account")
    page.get_by_role("textbox", name="Secret key").fill(account_id)
    page.get_by_role("button", name="Log in").click()
    expect(page.get_by_text("Membership: None")).to_be_visible()
    page.goto("http://localtest.me:8000/")
    yield page
