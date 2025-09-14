# Data Generation Script for the DataBricks Hackathon 
# This script generates synthetic transaction data using faker for the hackathon challenges.

# Import packages
import pandas as pd
from faker import Faker
import random

class TransactionDataGenerator:
    def __init__(self,
                 categories_ind: list,
                 categories_comp: list, 
                 num_accounts: int = 50,
                 seed: int = 42):
        """Initialize the data generator with categories and seed."""
        # Initialize Faker and random seed
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        # Transaction categories
        self.categories_ind = categories_ind
        self.categories_comp = categories_comp

        # Number of unique accounts to generate
        self.num_accounts = num_accounts

    def _generate_sepa_description(self) -> str:

        """Generate a random SEPA transaction description."""
        company = self.fake.company().upper()
        name = self.fake.name().upper()
        city = self.fake.city().upper()
        reference = self.fake.bothify(text="INV#####")
        ordernum = self.fake.bothify(text="ORD######")
        mandate = self.fake.bothify(text="MAND####")
        iban = self.fake.iban()

        templates = [
            f"TRANSFER FROM {name} {reference}",
            f"DIRECT DEBIT {company} {reference}",
            f"PAYMENT {company} {ordernum}",
            f"CREDIT {company} SALARY {self.fake.month_name().upper()}",
            f"CARD PAYMENT {company} {city}",
            f"ONLINE PAYMENT {company} ORDER {ordernum}",
            f"TRANSFER TO {name} {iban}",
            f"SEPA DD {company} MANDATE {mandate}",
            f"SUBSCRIPTION {company} {reference}",
            f"UTILITY BILL {company} {reference}",
            f"PURCHASE {company} {ordernum}"
        ]

        return random.choice(templates)

    
    def _add_balance_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a running balance column to the DataFrame.
        - For Individuals: random between 0 and 50,000
        - For Companies: random between 0 and 500,000
        - Balances update per transaction based on debit/credit
        """
        # Dictionary to store current balance per account
        account_balances = {}
        balances = []

        for idx, row in df.iterrows():
            account = row["main_account_number"]

            # Initialize balance if first time seeing the account
            if account not in account_balances:
                if row["organization_indicator"] == "Individual":
                    account_balances[account] = round(random.uniform(0, 50000), 2)
                else:  # Company
                    account_balances[account] = round(random.uniform(0, 500000), 2)

            # Update balance based on transaction type
            if row["debit_credit"] == "debit":
                account_balances[account] -= row["amount"]
            else:  # credit
                account_balances[account] += row["amount"]

            # Store the running balance for this transaction
            balances.append(round(account_balances[account], 2))

        df["balance"] = balances
        return df


    def generate_transaction_data(self, num_records: int) -> pd.DataFrame:
        """Generate synthetic transaction data with a fixed number of unique accounts."""
        data = []

        # Generate a fixed pool of customer relationship managers
        managers = [self.fake.name() for _ in range(15)]    

        # Generate a fixed pool of accounts
        accounts = []
        for _ in range(self.num_accounts):
            org_indicator = random.choice(['Individual', 'Company'])
            accounts.append({
                "main_account_number": self.fake.iban(),
                "organization_indicator": org_indicator,
                "business_responsible": random.choice(managers)
            })

        # Generate transactions assigned to the accounts
        for _ in range(num_records):
            account = random.choice(accounts)  # Pick an existing account
            transaction = {
                "transaction_id": self.fake.uuid4(),
                "customer_id": self.fake.uuid4(),
                "main_account_number": account["main_account_number"],
                "transaction_date": self.fake.date_between("-3y", "today"),
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "debit_credit": random.choice(['debit', 'credit']),
                "description": self._generate_sepa_description(),
                "merchant": self.fake.company(),
                "category": random.choice(
                    self.categories_ind if account["organization_indicator"] == "Individual" 
                    else self.categories_comp
                ),
                "payment_method": random.choice(['Credit Card', 'Debit Card', 'Mobile Payment']),
                "organization_indicator": account["organization_indicator"],
                "currency": "EUR",
                "business_responsible": account["business_responsible"]
            }
            data.append(transaction)

        # Create DataFrame and add balance column
        df = pd.DataFrame(data)

        # Sort by date
        df = df.sort_values(by="transaction_date")

        # Add running balance column
        df = self._add_balance_column(df)

        return df


if __name__ == "__main__":

    # Define categories for transactions
    categories_ind_ls = [
        'Groceries', 'Electronics', 'Clothing & Accessories', 'Dining & Restaurants',
        'Travel', 'Entertainment', 'Healthcare & Medical Expenses', 'Education & Tuition',
        'Utilities', 'Transportation', 'Fitness & Wellness', 'Home & Furniture',
        'Personal Care', 'Gifts & Donations', 'Childcare & Babysitting', 'Pet Care & Supplies',
        'Insurance', 'Housing & Rent', 'Financial Services',
    ]

    categories_comp_ls = [
        'Office Supplies', 'Software & Subscriptions', 'Professional Services',
        'Marketing & Advertising', 'Employee Benefits', 'Payroll & Salaries',
        'Travel & Hospitality', 'Utilities & Rent', 'Equipment & Maintenance',
        'Research & Development', 'Manufacturing & Raw Materials', 'Shipping & Logistics',
        'IT & Cloud Services', 'Security Services', 'Recruiting & Hiring',
        'Corporate Events & Sponsorships', 'Taxes & Compliance', 'Business Insurance',
        'Warehousing & Storage', 'Energy & Fuel'
    ]

    # Create an instance of the data generator
    generator = TransactionDataGenerator(categories_comp=categories_comp_ls, categories_ind=categories_ind_ls, num_accounts=800, seed=42)

    # Generate a sample of transaction data
    df = generator.generate_transaction_data(10000)

    # Save to CSV
    df.to_csv("synthetic_transaction_data.csv", index=False)


