import textwrap
from http.client import responses

import bcrypt
from supabase import create_client, Client
from flask import session
import os
import random
import string
import smtplib
from email.message import EmailMessage


class Settings:
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

    def get_business_info(self):
        """returns business information for that business"""
        try:
            response = self.supabase.table('business_information').select('*').execute()
            return response.data

        except Exception as e:
            print(f'Exception: {e}')

    def update_business_info(self, updated_data):
        """Updates the business information with inserted data"""
        try:
            # Get the current business info
            info = self.get_business_info()

            # Check if business info exists
            if not info or len(info) == 0:
                print("No business information found to update")
                return None

            info_id = info[0]['id']

            # Update the business information
            response = self.supabase.table('business_information').update(updated_data).eq('id', info_id).execute()

            # Check if the update was successful
            if response.data:
                print("Business information updated successfully")
                return True
            else:
                print("Update failed - no data returned")
                return False

        except KeyError as e:
            print(f'Missing key in business info: {e}')
            return False
        except Exception as e:
            print(f'Exception updating business info: {e}')
            return False


    def load_users(self):
        """returns users data"""
        try:
            response = self.supabase.table('users').select('*').execute()
            return response.data

        except Exception as e:
            print(f'Exception: {e}')

test = Settings()
print(
    test.load_users()
)
