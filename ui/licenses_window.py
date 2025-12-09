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
                "attribution": "The Data Investigation Multi-Tool was created by William Kenny in 2025. This software is made available to you under the terms of the Apache License 2.0.",
                "url": "http://www.apache.org/licenses/LICENSE-2.0",
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

This application incorporates open-source software. The following list shows each package and its licence.

------------------------------------------------------------

Jinja2 3.1.6 (BSD License)
https://github.com/pallets/jinja/

MarkupSafe 3.0.2 (BSD License)
https://github.com/pallets/markupsafe/

Pygments 2.19.2 (BSD License)
https://pygments.org

RapidFuzz 3.13.0
https://github.com/rapidfuzz/RapidFuzz

asttokens 3.0.0 (Apache 2.0)
https://github.com/gristlabs/asttokens

certifi 2025.7.14 (Mozilla Public License 2.0)
https://github.com/certifi/python-certifi

charset-normalizer 3.4.2 (MIT License)
https://github.com/jawah/charset_normalizer

colorama 0.4.6 (BSD License)
https://github.com/tartley/colorama

decorator 5.2.1 (BSD License)

executing 2.2.0 (MIT License)
https://github.com/alexmojaki/executing

idna 3.10 (BSD License)
https://github.com/kjd/idna

ipython 9.4.0 (BSD License)
https://ipython.org

ipython_pygments_lexers 1.1.1 (BSD License)
https://github.com/ipython/ipython-pygments-lexers

jaraco.classes 3.4.0 (MIT License)
https://github.com/jaraco/jaraco.classes

jaraco.context 6.0.1 (MIT License)
https://github.com/jaraco/jaraco.context

jaraco.functools 4.2.1
https://github.com/jaraco/jaraco.functools

jedi 0.19.2 (MIT License)
https://github.com/davidhalter/jedi

jsonpickle 4.1.1 (BSD-3-Clause)
https://jsonpickle.readthedocs.io/

keyring 25.6.0 (MIT License)
https://github.com/jaraco/keyring

matplotlib-inline 0.1.7 (BSD License)
https://github.com/ipython/matplotlib-inline

more-itertools 10.7.0 (MIT License)
https://github.com/more-itertools/more-itertools

networkx 3.5 (BSD License)
https://networkx.org/

parso 0.8.4 (MIT License)
https://github.com/davidhalter/parso

pillow 10.4.0 (Historical Permission Notice and Disclaimer)
https://python-pillow.org

prompt_toolkit 3.0.51 (BSD License)

pure_eval 0.2.3 (MIT License)
http://github.com/alexmojaki/pure_eval

pyvis 0.3.2 (BSD)
https://github.com/WestHealth/pyvis

pywin32-ctypes 0.2.3 (BSD-3-Clause)
https://github.com/enthought/pywin32-ctypes

requests 2.32.4 (Apache Software License)
https://requests.readthedocs.io

stack-data 0.6.3 (MIT License)
http://github.com/alexmojaki/stack_data

traitlets 5.14.3 (BSD License)
https://github.com/ipython/traitlets

ttkbootstrap 1.14.1 (MIT License)
https://github.com/israel-dryer/ttkbootstrap

urllib3 2.5.0
https://github.com/urllib3/urllib3
"""
        HelpWindow(self, "Third-Party Software Licenses", THIRD_PARTY_LICENSES)
