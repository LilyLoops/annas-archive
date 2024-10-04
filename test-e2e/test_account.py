from playwright.sync_api import Page, expect
import re


def test_create_account(page: Page):
    page.goto("http://localtest.me:8000/account")

    # wait for the page to load
    expect(page.get_by_role("heading", name="Log in / Register", exact=True)).to_be_visible()

    page.get_by_role("button", name="Register new account", exact=True).click()
    # bypass the flask-debug page
    expect(page).to_have_url(re.compile(r"/account/register$"))
    page.get_by_role("link").click()

    # get the user's secret key
    page.get_by_text("Your secret key is").wait_for()
    # TODO: edit the page to make the secret key easier to access programmatically
    secret_key = page.get_by_text("Your secret key is").locator('[class="font-bold underline"]').text_content()
    assert secret_key
    secret_key = secret_key.split()[0]

    # now log in with the secret key
    page.get_by_role("textbox", name="Secret key").fill(secret_key)
    page.get_by_role("button", name="Log in").click()

    # bypass the flask-debug page
    expect(page).to_have_url(re.compile(r"/account/$"))
    page.get_by_role("link").click()

    expect(page.get_by_text("Membership: None")).to_be_visible()


def test_create_account_programmatically(authenticated_page: Page):
    authenticated_page.goto("http://localtest.me:8000/account")
    expect(authenticated_page.get_by_text("Membership: None")).to_be_visible()
