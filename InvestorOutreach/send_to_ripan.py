#!/usr/bin/env python3
"""One-off: send the investor update to Ripan only, BCC Jai, no test banner."""

from send_investor_update import (
    build_investor_update_email, send_email, WEEKLY_UPDATE, BCC_EMAIL
)

subject = "Vora Update - Feb 2026"

html = build_investor_update_email(
    first_name="Ripan",
    subject=subject,
    update_content=WEEKLY_UPDATE,
    test_mode=False,
)

print(f"Sending to: ripankadakia@gmail.com")
print(f"BCC: {BCC_EMAIL}")
print(f"Subject: {subject}")
print()

status, error = send_email(
    to_email="ripankadakia@gmail.com",
    subject=subject,
    html_content=html,
    bcc_email=BCC_EMAIL,
)

if status and 200 <= status < 300:
    print(f"Sent successfully (status {status})")
else:
    print(f"Failed: {error}")
