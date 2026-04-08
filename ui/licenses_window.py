# ui/licenses_window.py
"""Licenses and attribution window."""

import tkinter as tk
from tkinter import ttk
import webbrowser

from .scrollable_frame import ScrollableFrame
from .help_window import HelpWindow

class LicensesWindow(tk.Toplevel):
    """A pop-up window to display API attributions and license links."""

    def __init__(self, parent):
        super().__init__(parent)
        self.title("Data Source Attribution & Licenses")
        self.transient(parent)  # Keep this window on top of the main app
        self.geometry("600x650")
        self.grab_set()  # Modal behavior

        # 1. Create an instance of our reusable ScrollableFrame
        scroller = ScrollableFrame(self)
        scroller.pack(expand=True, fill=tk.BOTH, padx=15, pady=15)

        # 2. Get the inner frame that we can add our widgets to
        content_frame = scroller.scrollable_frame

        # --- Define the data sources ---
        sources = [
            {
                "name": "Software License",
                "attribution": "Copyright (c) 2025 Crown Copyright. Created by William Kenny. This software is made available under the MIT License. Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files, to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, subject to the following conditions: The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.",
                "url": "https://opensource.org/licenses/MIT",
            },
            {
                "name": "Companies House",
                "attribution": "Information on the public register is made available by virtue of approvals issued by Companies House in accordance with section 47 of the Copyright, Designs and Patents Act 1988 and Schedule 1 of the Database Regulations (SI 1997/3032). Companies House imposes no rules or requirements on how the information on the public register is used.",
                "url": "https://www.gov.uk/government/publications/companies-house-accreditation-to-information-fair-traders-scheme/public-task-copyright-and-crown-copyright",
            },
            {
                "name": "Charity Commission",
                "attribution": "The API calls, API responses, and all other material on the Developer Hub website are subject to Crown Copyright and are made available to you subject to these Terms and the terms of the Open Government Licence v3.0.",
                "url": "https://api-portal.charitycommission.gov.uk/terms",
            },
            {
                "name": "360Giving",
                "attribution": "GrantNav data is published under the Creative Commons Attribution 4.0 International license. Additional terms and conditions can be found here: https://www.360giving.org/explore/api-tc/",
                "url": "https://creativecommons.org/licenses/by/4.0/",
            },
        ]

        # --- Create a section for each source ---
        for source in sources:
            frame = ttk.LabelFrame(content_frame, text=source["name"], padding=10)
            frame.pack(fill=tk.X, pady=5)

            attr_label = ttk.Label(
                frame, text=source["attribution"], wraplength=550, justify=tk.LEFT
            )
            attr_label.pack(anchor="w", pady=(0, 10))

            # Use a lambda to pass the correct URL to the button command
            url_button = ttk.Button(
                frame,
                text="View Full License",
                command=lambda u=source["url"]: webbrowser.open_new_tab(u),
                style="Link.TButton",
            )
            url_button.pack(anchor="e")

        ttk.Separator(content_frame).pack(fill=tk.X, pady=15)

        third_party_button = ttk.Button(
            content_frame,
            text="View Third-Party Software Licenses",
            command=self._show_third_party_licenses,
            bootstyle="info-outline",
        )
        third_party_button.pack(pady=5)

        # --- Add a close button ---
        close_button = ttk.Button(content_frame, text="Close", command=self.destroy)
        close_button.pack(side=tk.BOTTOM, pady=(20, 0))

    def _show_third_party_licenses(self):
        """Displays the embedded third-party license text."""

        THIRD_PARTY_LICENSES = """
--- THIRD-PARTY SOFTWARE LICENSES ---

This application incorporates open-source software. The following lists each
runtime dependency and its licence. All licences are permissive and compatible
with the MIT licence under which this application is distributed.

------------------------------------------------------------

certifi (Mozilla Public License 2.0)
https://github.com/certifi/python-certifi

charset-normalizer (MIT License)
https://github.com/jawah/charset_normalizer

colorama (BSD License)
https://github.com/tartley/colorama

contourpy (BSD License)
https://github.com/contourpy/contourpy

cycler (BSD License)
https://github.com/matplotlib/cycler

fonttools (MIT License)
https://github.com/fonttools/fonttools

idna (BSD-3-Clause)
https://github.com/kjd/idna

iniconfig (MIT License)
https://github.com/pytest-dev/iniconfig

jaraco.classes (MIT License)
https://github.com/jaraco/jaraco.classes

jaraco.context (MIT License)
https://github.com/jaraco/jaraco.context

jaraco.functools (MIT License)
https://github.com/jaraco/jaraco.functools

keyring (MIT License)
https://github.com/jaraco/keyring

kiwisolver (BSD License)
https://github.com/nucleic/kiwi

lxml (BSD-3-Clause)
https://github.com/lxml/lxml

matplotlib (Python Software Foundation License)
https://matplotlib.org

networkx (BSD-3-Clause)
https://networkx.org/

numpy (BSD-3-Clause)
https://numpy.org

packaging (Apache Software License / BSD License)
https://github.com/pypa/packaging

pandas (BSD License)
https://pandas.pydata.org

pgeocode (BSD License)
https://github.com/symerio/pgeocode

pillow (MIT-CMU / Historical Permission Notice and Disclaimer)
https://python-pillow.org

pyparsing (MIT License)
https://github.com/pyparsing/pyparsing

python-dateutil (Apache Software License / BSD License)
https://github.com/dateutil/dateutil

pytz (MIT License)
https://github.com/stub42/pytz

gravis (Apache 2.0 License)
https://github.com/robert-haas/gravis

pywin32-ctypes (BSD-3-Clause) [Windows only]
https://github.com/enthought/pywin32-ctypes

RapidFuzz (MIT License)
https://github.com/rapidfuzz/RapidFuzz

requests (Apache Software License)
https://requests.readthedocs.io

six (MIT License)
https://github.com/benjaminp/six

ttkbootstrap (MIT License)
https://github.com/israel-dryer/ttkbootstrap

tzdata (Apache-2.0)
https://github.com/python/tzdata

urllib3 (MIT License)
https://github.com/urllib3/urllib3
"""
        HelpWindow(self, "Third-Party Software Licenses", THIRD_PARTY_LICENSES)
