import textwrap

import bcrypt
from supabase import create_client, Client
from flask import session
import os
import random
import string
import smtplib
from email.message import EmailMessage


class UserAuthentication:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not service_role_key:
            raise ValueError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set.")

        self.supabase: Client = create_client(url, service_role_key)

        # email authentication
        self.sender_email = os.getenv('SENDER_EMAIL')
        self.email_password = os.getenv('EMAIL_PASSWORD')

    def sign_up(self, user_name, email, nrc_number, date_of_birth, user_type, password, secret_key):
        """Signs up a user using a valid secret key"""

        try:
            # Check if the secret key is valid
            keys_response = self.supabase.table('secret_keys').select('key').execute()
            keys = [info['key'] for info in keys_response.data]

            if secret_key not in keys:
                print('Invalid secret key')
                return {'error': 'Invalid secret key'}

            # Insert user with plain password (not recommended for production)
            data = {
                'user_name': user_name,
                'email': email,
                'nrc_number': nrc_number,
                'date_of_birth': date_of_birth,
                'user_type': user_type,
                'password': password
            }

            response = self.supabase.table('users').insert(data).execute()

            # Optionally: delete or mark the secret key as used
            # self.supabase.table('secret_keys').delete().eq('key', secret_key).execute()

            return {'success': True, 'user': response.data}

        except Exception as e:
            print(f'Exception: {e}')
            return {'error': str(e)}

    def login(self, email, password):
        """Logs the user into the platform and returns user_type on success"""

        try:
            # Fetch user record including user_type
            user_response = self.supabase.table('users') \
                .select('email, password, user_type') \
                .eq('email', email) \
                .execute()

            users = user_response.data

            if not users:
                print('Email not found')
                return {'error': 'Email not found'}

            user = users[0]
            stored_password = user['password']

            if password != stored_password:
                print('Invalid password')
                return {'error': 'Invalid password'}

            # Login successful
            return {
                'success': True,
                'user_type': user['user_type']
            }

        except Exception as e:
            print(f'Exception: {e}')
            return {'error': str(e)}


