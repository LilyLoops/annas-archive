from playwright.sync_api import Page, expect
import re


def test_has_title(page: Page):
    page.goto("http://localtest.me:8000/")

    # Expect a title "to contain" a substring.
    expect(page).to_have_title(re.compile("Anna’s Archive"))
