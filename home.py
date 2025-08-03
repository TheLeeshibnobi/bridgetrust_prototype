import textwrap

import bcrypt
from supabase import create_client, Client
from flask import session
import os
import random
import string
import smtplib
from email.message import EmailMessage


class Home:
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

    def total_principal_given(self):
        """Returns the total amount of principal given out as loans."""
        try:
            response = (
                self.supabase
                .table('loans')
                .select('loan_amount')
                .execute()
            )

            loan_data = response.data or []
            total = sum(float(loan.get('loan_amount', 0)) for loan in loan_data)

            return round(total, 2)

        except Exception as e:
            print(f"[total_principal_given] Error: {e}")
            return 0.0

    def interest_earned(self):
        """returns the total interest earned"""
        try:
            response = (
                self.supabase
                .table('loan_repayments')
                .select('interest_component')
                .execute()
            )

            repayment_data = response.data or []
            total = sum(float(loan.get('interest_component', 0)) for loan in repayment_data)

            return round(total, 2)

        except Exception as e:
            print(f"[total_interest_earned] Error: {e}")
            return 0.0

    def total_receivables(self) -> float:
        """
        Calculate and return the total receivables.
        Total receivables = total principal given + total interest earned

        Returns:
            float: The total receivables, or 0.0 if an error occurs.
        """
        try:
            principal = self.total_principal_given() or 0.0
            interest = self.interest_earned() or 0.0
            return round(principal + interest, 2)
        except Exception as e:
            print(f"[ERROR] Failed to calculate total receivables: {e}")
            return 0.0


