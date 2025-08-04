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

    def exhausted_loan_request_data(self, status):
        """
        Gets a full loan request data with:
        - personal_information (from borrowers table)
        - loan_information (from loan_requests table)
        - next_of_kin_information (from next_of_kins table)
        - loan_files (from loan_files table)
        - borrower_files (from borrower_files table)
        - organisation_information (from organisations table)
        """
        try:
            loan_response = (
                self.supabase
                .table('loan_requests')
                .select('*')
                .eq('status', status)
                .execute()
            )

            loan_requests = loan_response.data
            full_data = []

            for loan in loan_requests:
                borrower_id = loan.get('borrower_id')
                loan_file_id = loan.get('loan_file_id')

                # Get borrower info
                borrower_info = (
                    self.supabase
                    .table('borrowers')
                    .select('*')
                    .eq('id', borrower_id)
                    .single()
                    .execute()
                ).data if borrower_id else {}

                # Get next of kin info
                next_of_kin_info = {}
                if borrower_info and borrower_info.get('next_of_kin_id'):
                    next_of_kin_id = borrower_info['next_of_kin_id']
                    next_of_kin_info = (
                        self.supabase
                        .table('next_of_kins')
                        .select('first_name, last_name, email, phone')
                        .eq('id', next_of_kin_id)
                        .single()
                        .execute()
                    ).data

                # Get organisation info
                organisation_info = {}
                if borrower_info and borrower_info.get('organisation_id'):
                    organisation_id = borrower_info['organisation_id']
                    organisation_info = (
                        self.supabase
                        .table('organisations')
                        .select('*')
                        .eq('id', organisation_id)
                        .single()
                        .execute()
                    ).data

                # Get loan files info
                loan_files = (
                    self.supabase
                    .table('loan_files')
                    .select('*')
                    .eq('id', loan_file_id)
                    .single()
                    .execute()
                ).data if loan_file_id else {}

                # Get borrower files
                borrower_files = (
                    self.supabase
                    .table('borrower_files')
                    .select('*')
                    .eq('borrower_id', borrower_id)
                    .execute()
                ).data if borrower_id else []

                # Append all data
                full_data.append({
                    'personal_information': borrower_info,
                    'loan_information': loan,
                    'next_of_kin_information': next_of_kin_info,
                    'organisation_information': organisation_info,
                    'loan_files': loan_files,
                    'borrower_files': borrower_files
                })

            return full_data

        except Exception as e:
            print(f'Exception:: {e}')
            return []

    def reject_loan_request(self, loan_request_id):
        """Updates the status of a specific loan request to 'rejected'."""
        try:
            response = (
                self.supabase
                .table('loan_requests')
                .update({'status': 'rejected'})
                .eq('id', loan_request_id)
                .execute()
            )
            return response.data

        except Exception as e:
            print(f'Exception: {e}')



