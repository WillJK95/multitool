# multitool/constants.py
"""Shared constants and configuration values."""

import os

# --- API Configuration ---
API_BASE_URL = "https://api.company-information.service.gov.uk"
GRANTNAV_API_BASE_URL = "https://api.threesixtygiving.org/api/v1"
CHARITY_API_BASE_URL = "https://api.charitycommission.gov.uk/register/api"
CONTRACTS_FINDER_BASE_URL = "https://www.contractsfinder.service.gov.uk"

# --- File Paths ---
CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".multitool")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.ini")

# --- Keyring Configuration ---
SERVICE_NAME = "multitool"
CH_ACCOUNT_NAME = "CompaniesHouseAPI"
CC_ACCOUNT_NAME = "CharityCommissionAPI"

# --- API Rate Limiting Defaults ---
DEFAULT_CH_PACING_MODE = "smooth"       # "smooth" or "burst"
INITIAL_RATE_LIMIT = 590                # conservative startup value (before first API response)
SMOOTH_BURST_WINDOW_SECONDS = 15        # seconds of tokens the smooth-mode bucket holds
SMOOTH_SAFETY_MARGIN = 0.90             # fraction of server rate used in smooth mode
DEFAULT_CH_MAX_WORKERS = 2              # concurrent API threads
MAX_CH_MAX_WORKERS = 8                  # hard ceiling for workers
MIN_CH_MAX_WORKERS = 1                  # floor for workers

# --- Default Values ---
DEFAULT_ITEMS_PER_PAGE = 100
DEFAULT_FUZZY_THRESHOLD = 85
DEFAULT_MAX_WORKERS = 2
DEFAULT_MAX_RETRIES = 4
DEFAULT_BACKOFF_FACTOR = 0.5

# --- Field Definitions ---
COMPANY_DATA_FIELDS = {
    "company_number": "Company Number",
    "incorporation_date": "Incorporation Date",
    "company_status": "Company Status",
    "registered_address": "Registered Address",
    "sic_codes": "SIC Codes",
    "officers": "Officers",
    "persons_with_significant_control": "Persons with Significant Control (PSCs)",
    "company_type": "Company Type",
    "jurisdiction": "Jurisdiction",
    "date_of_cessation": "Date of Cessation",
    "previous_company_names": "Previous Company Names",
    "accounts_next_due": "Accounts - Next Due",
    "accounts_last_made_up_to": "Accounts - Last Made Up To",
    "confirmation_statement_next_due": "Confirmation Statement - Next Due",
    "confirmation_statement_last_made_up_to": "Confirmation Statement - Last Made Up To",
    "accounts_type": "Accounts Type",
}

GRANT_DATA_FIELDS = {
    "title": "Title",
    "description": "Description",
    "amountAwarded": "Amount Awarded",
    "currency": "Currency",
    "awardDate": "Award Date",
    "fundingOrganization_name": "Funder Name",
    "fundingOrganization_website": "Funder Website",
    "grantProgramme_title": "Grant Programme",
    "plannedDates_startDate": "Grant Start Date",
    "plannedDates_endDate": "Grant End Date",
    "plannedDates_durationExpression": "Grant Duration (months)",
    "beneficiaryLocation_name": "Beneficiary Location",
}

CHARITY_DATA_FIELDS = {
    "reg_charity_number": "Charity Number",
    "main_details": "Main Charity Details (Address, Phone etc.)",
    "date_of_registration": "Date of Registration",
    "other_names": "Other Names",
    "trustee_names": "Trustee Names",
    "financial_history": "Financial History (Last 5 Years)",
    "assets_liabilities": "Most Recent Assets & Liabilities",
    "annual_return_overview": "Annual Return Overview",
    "other_regulators": "Other Regulators",
    "regulatory_reports": "Regulatory Reports & Inquiries",
    "area_of_operation": "Area of Operation",
    "filing_information": "Financial Filing Information",
    "removal_info": "Removal Status & History",
    "governance_status": "Governance & Insolvency Status",
}

# --- Filing History Type Categories ---
FILING_TYPE_CATEGORIES = {
    'AA': 'Accounts Filed',
    'CS01': 'Confirmation Statement',
    'GAZ1': 'First Gazette (Strike-off)',
    'GAZ2': 'Second Gazette (Strike-off)',
    'CH01': 'Change of Name',
    'CH02': 'Change of Name',
    'LIQEN': 'Liquidation',
    'LIQEO': 'Liquidation',
    'AD01': 'Administration',
    'AD02': 'Administration',
    'DS01': 'Striking Off Application',
    'CERTR': 'Restoration to Register',
    '600': 'Voluntary Arrangement',
    'RECAD': 'Receiver/Manager Appointed',
    'NEWINC': 'Incorporation',
    'MR01': 'Charge Registered',
    'MR04': 'Charge Satisfied',
    'MR05': 'Charge Satisfied',
    'SH01': 'Allotment of Shares',
    'RES': 'Special Resolution',
}

# --- Accounts Constants and Taxonomies ---
TAXONOMY_MAP = {
    'NetAssets': [
        'NetAssetsLiabilities',
        'TotalAssetsLessCurrentLiabilities',
    ],
    'CurrentAssets': [
        'CurrentAssets'
    ],
    'CurrentLiabilities': [
        'Creditors',
        'CreditorsAmountsFallingDueWithinOneYear'
    ],
    'Revenue': [
        'Revenue',
        'Turnover'
    ],
    'ProfitLoss': [
        'ProfitLoss'
    ],
    'FixedAssets': [
        'FixedAssets',
        'PropertyPlantEquipment'
    ],
    'Debtors': [
        'Debtors'
    ],
    'CashBankInHand': [
        'CashBankInHand'
    ],
    'Employees': [
        'AverageNumberEmployeesDuringPeriod'
    ],
    'TotalAssets': [
        'TotalAssets'
    ],
    'ShareCapital': [
        'ShareCapital'
    ],
    'RetainedEarnings': [
        'RetainedEarningsAccumulatedLosses'
    ],
    'IntangibleAssets': [
        'IntangibleAssets'
    ],
    'TangibleAssets': [
        'TangibleFixedAssets',
        'PropertyPlantEquipment'
    ],
    'CreditorsAfterOneYear': [
        'CreditorsAmountsFallingDueAfterOneYear'
    ],
    'NetCurrentAssets': [
        'NetCurrentAssetsLiabilities'
    ],
}

# --- Manual Input Field Definitions for EDD Cross-Analysis ---
# Tuples: (manual_field_key, auto_field_key_or_None, display_label)
MANUAL_INPUT_FIELDS_TIER1 = [
    ('Turnover', 'Revenue', 'Turnover / Revenue'),
    ('PreTaxProfitLoss', 'ProfitLoss', 'Pre-tax Profit/Loss'),
    ('CashAtBank', 'CashBankInHand', 'Cash at Bank'),
    ('DirectorLoans', None, 'Director Loans (amount owed)'),
]

MANUAL_INPUT_FIELDS_TIER2 = [
    ('ManualDebtors', 'Debtors', 'Debtors'),
    ('StockInventory', None, 'Stock / Inventory'),
    ('DeferredIncome', None, 'Deferred Income'),
    ('CapitalisedDevCosts', None, 'Capitalised Development Costs'),
]

PAYMENT_MECHANISMS = ['Unknown', 'Advance', 'Arrears', 'Milestone-based']

IXBRL_NAMESPACES = {
    'ix': 'http://www.xbrl.org/2013/inlineXBRL',
    'ixt': 'http://www.xbrl.org/inlineXBRL/transformation/2015-02-26',
    'link': 'http://www.xbrl.org/2003/linkbase',
    'xbrli': 'http://www.xbrl.org/2003/instance',
    'core': 'http://xbrl.frc.org.uk/fr/2021-01-01/core',
    'bus': 'http://xbrl.frc.org.uk/cd/2021-01-01/business',
}
