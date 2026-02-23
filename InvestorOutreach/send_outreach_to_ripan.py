#!/usr/bin/env python3
"""One-off: send all 4 outreach variants to Ripan for feedback, BCC Jai."""

import time
from send_investor_outreach import (
    build_outreach_email, send_email, INVESTOR_TYPES, BCC_EMAIL
)

TO = "ripankadakia@gmail.com"
NAME = "Ripan"

print(f"Sending all 4 outreach variants to: {TO}")
print(f"BCC: {BCC_EMAIL}\n")

for inv_type in ['vc', 'accelerator', 'angel', 'stakeholder']:
    html, subject = build_outreach_email(
        first_name=NAME,
        investor_type=inv_type,
        test_mode=False,
    )

    label = INVESTOR_TYPES[inv_type]['label']
    print(f"  {label}: {subject}")

    status, error = send_email(
        to_email=TO,
        subject=subject,
        html_content=html,
        bcc_email=BCC_EMAIL,
    )

    if status and 200 <= status < 300:
        print(f"    Sent (status {status})")
    else:
        print(f"    Failed: {error}")

    time.sleep(1)

print("\nDone.")
