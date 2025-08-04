import textwrap
from http.client import responses
import uuid
from datetime import datetime
import mimetypes

import bcrypt
from supabase import create_client, Client
from flask import session
import os
import random
import string
import smtplib
from email.message import EmailMessage


class Notifications:
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

    def load_loan_request(self):
        """Returns data for the loan requests"""
        try:
            response = (
                self.supabase
                .table('loan_requests')
                .select('*')
                .eq('status', 'pending')
                .execute()
            )
            return response.data
        except Exception as e:
            print(f'Exception while loading loan requests: {e}')
            return None

    def formulate_notification(self, loan):
        """
        Returns a dictionary containing:
        - notification string
        - borrower_id
        - loan_request_id (as request_id)
        - user_id
        """
        principal = loan.get('principal')
        months_tenure = loan.get('months_tenure')
        total_payable = loan.get('total_payable')
        user_id = loan.get('user_id')

        # 🔍 Query users table to get the officer's name
        try:
            user_response = (
                self.supabase
                .table('users')
                .select('user_name')
                .eq('id', user_id)
                .single()
                .execute()
            )
            loan_officer = user_response.data.get('user_name')
        except Exception as e:
            print(f"Error fetching user_name for user_id {user_id}: {e}")
            loan_officer = "Unknown Officer"

        # 📝 Build the notification message
        message = (
            f"📢 Loan Application Pending Approval\n"
            f"A loan of ZMW {principal:,.2f} for {months_tenure} months is awaiting approval. "
            f"Total payable: {total_payable:,.2f}. Officer: {loan_officer}."
        )

        # 📤 Return the single notification as a dictionary
        return {
            'notification': message,
            'borrower_id': loan.get('borrower_id'),
            'request_id': loan.get('id'),
            'user_id': user_id
        }

    def store_notification(self, notification_data):
        """Stores a list of notifications to the database"""

        try:
            # insert all notifications at once
            response = self.supabase.table('notifications').insert(notification_data).execute()
            return response  # optional: return response to confirm insert

        except Exception as e:
            print(f'Exception while storing notifications: {e}')
            return None

    def load_notifications(self):
        """loads notifications from the notification table"""

        try:
            response = (
                self.supabase
                .table('notifications')
                .select('*')
                .execute()
            )
            return response.data
        except Exception as e:
            print(f'Exception while loading loan requests: {e}')
        return None

    def exhausted_loan_request_data(self):
        """gets an exhusted laon """

test = Notifications()
print(test.load_notifications())