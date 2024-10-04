from playwright.sync_api import Page, expect


def test_donate_amazon(authenticated_page: Page):
    page = authenticated_page
    page.goto("http://localtest.me:8000/")

    page.locator('[class=header-bar]').get_by_role("link", name="Donate", exact=True).click()

    page.get_by_text("BrilliantBookworm", exact=True).locator('..').get_by_role("button", name="Join", exact=True).click()
    expect(page.get_by_text("Please select a payment method.", exact=True)).to_be_visible()

    page.get_by_role("button", name="Amazon Gift Card", exact=True).click()
    expect(page.get_by_text("This payment method requires a minimum of $10 USD. Please select a different duration or payment method.", exact=True)).to_be_visible()

    page.get_by_role("button", name="3 months -5%").click()
    page.get_by_role("button", name="Donate $20 for 3 months “Brilliant Bookworm”", exact=True).click()

    expect(page.get_by_text("Status: unpaid")).to_be_visible()
    expect(page.get_by_text("Status: Waiting for gift card…")).to_be_visible()
