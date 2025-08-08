import textwrap
from http.client import responses

import bcrypt
from numpy.ma.core import repeat
from supabase import create_client, Client
from flask import session
import os
import random
import string
import smtplib
from email.message import EmailMessage
import uuid


class Wallet:
    """contains methods required for the home template"""
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not service_role_key:
            raise ValueError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set.")

        self.supabase: Client = create_client(url, service_role_key)

        # email authentication
        self.sender_email = os.getenv('SENDER_EMAIL')
        self.email_password = os.getenv('EMAIL_PASSWORD')

    def load_transactions(self):
        """loads transactions in the wallet table"""
        try:
            response = (
                self.supabase
                .table('wallet')
                .select('*')
                .execute()
            )

            return response.data

        except Exception as e:
            print(f'Exception: {e}')

    def wallet_balance(self):
        """Fetches and returns the latest wallet balance."""
        try:
            response = (self.supabase
                        .table('wallet')
                        .select('balance')
                        .limit(1)
                        .order('created_at', desc=True)
                        .execute()
                    )
            # Ensure the response has data
            if not response.data or len(response.data) == 0:
                print("No wallet data found.")
                return 0.00

            balance = response.data[0].get('balance', 0.00)
            return balance

        except Exception as e:
            print(f"Error fetching wallet balance: {e}")
            return 0.00


    def compare_balance(self, amount):
        """compares the amount being withdrawn and the current balance"""

        if amount > self.wallet_balance():
            return False
        return True

    def generate_transaction_number(self):
        return f"TXN-{uuid.uuid4().hex[:12].upper()}"

    def insert_withdraw(self, amount, bank_name, account_number, company_name, swift_code, branch_info):
        """Inserts the withdrawn amount into the database if balance is sufficient."""

        # Get the current balance
        balance = self.wallet_balance()

        if amount > balance:
            return False, 'Amount requested is higher than balance available'

        new_balance = round(balance - amount, 2)

        # Fetch existing transaction numbers
        try:
            response = self.supabase.table('wallet').select('transaction_number').execute()
            transaction_numbers = [txn['transaction_number'] for txn in response.data]
        except Exception as e:
            return False, f'Failed to fetch transaction numbers: {e}'

        # Generate a unique transaction number
        max_attempts = 5
        for _ in range(max_attempts):
            transaction_number = self.generate_transaction_number()
            if transaction_number not in transaction_numbers:
                break
        else:
            return False, 'Unable to generate a unique transaction number after multiple attempts'

        # Prepare withdraw data
        withdraw_data = {
            'transaction_number': transaction_number,
            'transaction_type': 'cash_withdraw',
            'description': f'withdraw to {bank_name},{account_number},{company_name},{swift_code},{branch_info}',
            'status': 'pending',
            'amount': amount,
            'balance': new_balance
        }

        # Insert into the database
        try:
            wallet_response = self.supabase.table('wallet').insert(withdraw_data).execute()
            return True, wallet_response.data
        except Exception as e:
            return False, f'Error inserting withdrawal: {e}'


test = Wallet()
print(test.load_transactions())