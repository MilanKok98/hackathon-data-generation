# hackathon-data-generation

## Overview

This repository contains a Python script for generating **synthetic transaction data** for the upcoming **DataBricks Hackathon**. The goal is to create realistic financial transaction datasets that simulate individual and company behavior, suitable for testing analytics, machine learning, and fraud detection pipelines.

The script uses `Faker` and randomization techniques to generate a configurable number of transactions across a fixed number of accounts, including running balances and realistic transaction categories.

---

## Features

- Generate synthetic transactions for **Individuals** and **Companies**.
- Assign **categories** based on the organization type:
  - **Individuals**: groceries, entertainment, healthcare, etc.  
  - **Companies**: office supplies, payroll, marketing, etc.
- Generate realistic **SEPA-style transaction descriptions**.
- Support for multiple **payment methods** (Credit Card, Debit Card, Mobile Payment).
- Assign **running balances** per account, updating with debit and credit transactions.
- Specify a fixed number of **unique accounts** for consistency across datasets.
- Control **transaction date range** (up to the past 3 years).
- Output dataset as a **CSV file** for easy analysis.