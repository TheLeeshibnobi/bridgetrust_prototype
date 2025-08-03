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
import pandas as pd
from datetime import datetime, timedelta
import os


class Loans:
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

    def organisation_summary(self):
        """Returns summary of total loan and repayment stats grouped by organisation."""
        try:
            # Step 1: Get all loans
            loan_response = self.supabase.table('loans').select('id, organisation_id').execute()
            if loan_response.data is None:
                return []

            loans = loan_response.data

            # Step 2: Map loan_id to organisation_id
            loan_to_org = {}
            org_loan_count = {}
            for loan in loans:
                loan_id = loan['id']
                org_id = loan['organisation_id']
                loan_to_org[loan_id] = org_id
                org_loan_count[org_id] = org_loan_count.get(org_id, 0) + 1

            # Step 3: Get unique organisation_ids
            organisation_ids = list(org_loan_count.keys())

            # Step 4: Fetch organisation names
            org_response = (
                self.supabase
                .table('organisations')
                .select('id, name')
                .in_('id', organisation_ids)
                .execute()
            )
            org_map = {org['id']: org['name'] for org in org_response.data}

            # Step 5: Fetch all repayments
            loan_ids = list(loan_to_org.keys())
            repayment_response = (
                self.supabase
                .table('loan_repayments')
                .select('loan_id, principal_component, interest_component, balance')
                .in_('loan_id', loan_ids)
                .execute()
            )

            # Step 6: Aggregate by organisation
            from collections import defaultdict
            org_summary = defaultdict(lambda: {
                'total_principal_component': 0,
                'total_interest_component': 0,
                'total_balance': 0,
                'total_loans': 0,
                'organisation_id': '',
                'organisation_name': ''
            })

            for repayment in repayment_response.data:
                loan_id = repayment['loan_id']
                org_id = loan_to_org.get(loan_id)

                if not org_id:
                    continue

                org_entry = org_summary[org_id]
                org_entry['organisation_id'] = org_id
                org_entry['organisation_name'] = org_map.get(org_id, 'Unknown')
                org_entry['total_principal_component'] += repayment['principal_component']
                org_entry['total_interest_component'] += repayment['interest_component']
                org_entry['total_balance'] += repayment['balance']

            # Step 7: Add loan count to each org summary
            for org_id, count in org_loan_count.items():
                org_summary[org_id]['total_loans'] = count

            return list(org_summary.values())

        except Exception as e:
            print(f'Exception: {e}')
            return []

    def organisation_revenue_and_balance(self, organisational_id=None):
        """
        Returns total revenue (payment_amount) and total balance.
        Filters by organisational_id using the loans table via loan_id.
        """
        try:
            # Step 1: Get all loan IDs for the organisation
            loan_response = (
                self.supabase
                .table('loans')
                .select('id')
                .eq('organisation_id', organisational_id)
                .execute()
            )

            if not loan_response.data:
                return {'total_revenue': 0, 'total_balance': 0}

            loan_ids = [loan['id'] for loan in loan_response.data]

            if not loan_ids:
                return {'total_revenue': 0, 'total_balance': 0}

            # Step 2: Get repayments for those loan_ids
            repayment_response = (
                self.supabase
                .table('loan_repayments')
                .select('payment_amount, balance, loan_id')
                .in_('loan_id', loan_ids)
                .execute()
            )

            if not repayment_response.data:
                return {'total_revenue': 0, 'total_balance': 0}

            total_revenue = sum([r.get('payment_amount', 0) for r in repayment_response.data])
            total_balance = sum([r.get('balance', 0) for r in repayment_response.data])

            return {
                'total_revenue': total_revenue,
                'total_balance': total_balance
            }

        except Exception as e:
            print(f"[organisation_revenue_and_balance] Exception: {e}")
            return {
                'total_revenue': 0,
                'total_balance': 0
            }

    def organisations_loans(self, organisation_id):
        """Returns a list of loan summaries for borrowers in a specific organization"""
        try:
            # Get all loans for the organization
            loan_response = (
                self.supabase
                .table('loans')
                .select('*')
                .eq('organisation_id', organisation_id)
                .execute()
            )

            loans_data = loan_response.data
            loan_summaries = []

            for loan in loans_data:
                loan_id = loan['id']
                borrower_id = loan['borrower_id']
                loan_amount = loan['loan_amount']
                interest = loan['interest_rate']
                issue_date = loan.get('created_at', '')[:10]


                # Get borrower name
                borrower_response = (
                    self.supabase
                    .table('borrowers')
                    .select('first_name', 'last_name')
                    .eq('id', borrower_id)
                    .single()
                    .execute()
                )

                borrower_data = borrower_response.data
                borrower_name = f"{borrower_data['first_name']} {borrower_data['last_name']}"

                # Get repayment summary
                repayment_response = (
                    self.supabase
                    .table('loan_repayments')
                    .select('payment_amount', 'balance')
                    .eq('loan_id', loan_id)
                    .execute()
                )

                print(f"Repayments for loan {loan_id}: {repayment_response.data}")

                paid_total = 0
                balance_total = 0

                for rep in repayment_response.data:
                    paid_total += rep.get('payment_amount', 0)
                    balance_total += rep.get('balance', 0)

                # Assemble summary
                summary = {
                    'loan_id': loan_id,
                    'borrower_name': borrower_name,
                    'loan_amount': loan_amount,
                    'interest': interest,
                    'paid_amount': paid_total,
                    'balance': balance_total,
                    'issue_date': issue_date
                }

                loan_summaries.append(summary)

            return loan_summaries

        except Exception as e:
            print(f'Exception: {e}')
            return []

    def active_organisational_borrowers(self, organisation_id):
        """Returns the number of unique borrowers who have an active loan status under a specific organisation."""
        try:
            loan_response = (
                self.supabase
                .table('loans')
                .select('borrower_id')
                .eq('organisation_id', organisation_id)
                .eq('status', 'active')
                .execute()
            )

            if not loan_response.data:
                return 0

            borrowers = {item['borrower_id'] for item in loan_response.data if 'borrower_id' in item}
            return len(borrowers)

        except Exception as e:
            print(f'[active_organisational_borrowers] Exception: {e}')
            return 0


    def get_borrower_by_loan(self, loan_id):
        """gets the borrower info using the loan id"""
        try:
            # Get loan details including organisation_id and borrower_id
            loan_response = (
                self.supabase
                .table('loans')
                .select('borrower_id, organisation_id')
                .eq('id', loan_id)
                .execute()
            )

            loan_data = loan_response.data[0]
            borrower_id = loan_data['borrower_id']
            organisation_id = loan_data['organisation_id']

            # Get borrower info
            borrower_response = (
                self.supabase
                .table('borrowers')
                .select('*')
                .eq('id', borrower_id)
                .execute()
            )
            borrower = borrower_response.data[0]

            # Get organisation name
            org_response = (
                self.supabase
                .table('organisations')
                .select('name')
                .eq('id', organisation_id)
                .execute()
            )
            organisation_name = org_response.data[0]['name']

            # Add organisation name to borrower dictionary
            borrower['organisation'] = organisation_name

            return borrower


        except Exception as e:
            print(f'[active_organisational_borrowers] Exception: {e}')

    def get_loan_info_by_id(self, loan_id):
        """Gets loan information by loan_id, and includes user_name from users table"""
        try:
            # Step 1: Get loan data
            loan_response = (
                self.supabase
                .table('loans')
                .select('*')
                .eq('id', loan_id)
                .execute()
            )

            if not loan_response.data:
                return None  # Or handle as you like

            loan = loan_response.data[0]
            user_id = loan.get('user_id')

            # Step 2: Get user_name using user_id
            user_response = (
                self.supabase
                .table('users')
                .select('user_name')
                .eq('id', user_id)
                .execute()
            )

            if user_response.data:
                loan['user_name'] = user_response.data[0]['user_name']
            else:
                loan['user_name'] = None  # or 'Unknown'

            return loan

        except Exception as e:
            print(f'Exception: {e}')
            return None

    def verify_borrower(self, nrc_number, organisation_id):
        """Verifies if the borrower with this NRC belongs to the given organization."""
        try:
            response = (
                self.supabase
                .table('borrowers')
                .select('*')
                .eq('nrc_number', nrc_number)
                .eq('organisation_id', organisation_id)
                .execute()
            )

            if not response.data:
                return {
                    'status': False,
                    'message': 'Borrower not found',
                    'data': []
                }

            return {
                'status': True,
                'data': response.data
            }

        except Exception as e:
            print(f'Exception: {e}')
            return {
                'status': False,
                'message': f'Error: {str(e)}',
                'data': []
            }

    def loan_packages(self):
        """returns the nominal rate of return set by the business from the database"""
        try:
            response = self.supabase.table('nominal_rate').select('nominal_rate').execute()
            if response.data and len(response.data) > 0:
                return response.data[0]['nominal_rate']
            else:
                print("No nominal rate found in database")
                return None
        except Exception as e:
            print(f'Exception: {e}')
            return None

    def calculate_effective_rate_from_payments(self, principal, total_payments, days):
        """Calculate the effective interest rate based on actual payments made."""
        try:
            if isinstance(principal, str):
                principal = float(principal)
            if isinstance(total_payments, str):
                total_payments = float(total_payments)
            if isinstance(days, str):
                days = float(days)

            if principal <= 0 or total_payments <= 0 or days <= 0:
                return {
                    'status': False,
                    'message': 'Invalid input values: all values must be positive'
                }

            # Calculate total interest earned
            total_interest = total_payments - principal

            # Calculate effective rate as a percentage
            # Effective Rate = (Total Interest / Principal) * 100
            effective_rate = (total_interest / principal) * 100

            # For annualized effective rate (optional)
            time_in_years = days / 365
            annualized_effective_rate = ((total_payments / principal) ** (1 / time_in_years) - 1) * 100

            return {
                'status': True,
                'effective_rate': round(effective_rate, 2),
                'annualized_effective_rate': round(annualized_effective_rate, 2),
                'total_interest': round(total_interest, 2),
                'time_in_years': round(time_in_years, 2)
            }

        except Exception as e:
            print(f'Exception: {e}')
            return {
                'status': False,
                'message': str(e)
            }

    def get_loan_effective_rate_from_repayments(self, loan_id):
        """Calculate the actual effective rate for a loan based on recorded repayments."""
        try:
            # Get loan details
            loan_response = (
                self.supabase
                .table('loans')
                .select('loan_amount, created_at')
                .eq('id', loan_id)
                .execute()
            )

            if not loan_response.data:
                return {
                    'status': False,
                    'message': 'Loan not found'
                }

            loan_data = loan_response.data[0]
            principal = loan_data['loan_amount']
            loan_start_date = datetime.strptime(loan_data['created_at'][:10], '%Y-%m-%d')

            # Get all repayments for this loan
            repayment_response = (
                self.supabase
                .table('loan_repayments')
                .select('payment_amount, payment_date')
                .eq('loan_id', loan_id)
                .execute()
            )

            if not repayment_response.data:
                return {
                    'status': False,
                    'message': 'No repayments found for this loan'
                }

            # Calculate total payments and determine loan duration
            total_payments = sum([r.get('payment_amount', 0) for r in repayment_response.data])

            # Find the latest payment date to determine actual loan duration
            payment_dates = [datetime.strptime(r['payment_date'][:10], '%Y-%m-%d')
                             for r in repayment_response.data if r.get('payment_date')]

            if not payment_dates:
                # If no payment dates, use current date
                latest_payment_date = datetime.now()
            else:
                latest_payment_date = max(payment_dates)

            # Calculate actual loan duration in days
            actual_days = (latest_payment_date - loan_start_date).days

            # Calculate effective rate
            effective_result = self.calculate_effective_rate_from_payments(
                principal, total_payments, actual_days
            )

            if effective_result['status']:
                effective_result.update({
                    'loan_id': loan_id,
                    'principal': principal,
                    'total_payments': total_payments,
                    'actual_duration_days': actual_days,
                    'loan_start_date': loan_start_date.strftime('%Y-%m-%d'),
                    'latest_payment_date': latest_payment_date.strftime('%Y-%m-%d')
                })

            return effective_result

        except Exception as e:
            print(f'Exception: {e}')
            return {
                'status': False,
                'message': str(e)
            }

    def determine_monthly_payment(self, principal, days, method='amortisation'):
        """Determine the monthly payment using the monthly nominal rate from database."""
        try:
            # Convert days to integer/float if it's a string
            if isinstance(days, str):
                days = float(days)
            elif not isinstance(days, (int, float)):
                raise ValueError(f"Days parameter must be a number, got {type(days)}")

            # Convert principal to float if it's a string
            if isinstance(principal, str):
                principal = float(principal)
            elif not isinstance(principal, (int, float)):
                raise ValueError(f"Principal parameter must be a number, got {type(principal)}")

            # Get monthly rate from database (already in decimal form and already monthly)
            monthly_rate_decimal = self.loan_packages()
            if monthly_rate_decimal is None:
                return {
                    'status': False,
                    'message': 'Could not retrieve monthly rate from database'
                }

            total_months = round(days / 30)

            # For very short terms (less than 30 days), treat as partial month
            if days < 30:
                total_months = 1
                # Adjust rate proportionally for partial month
                actual_rate = monthly_rate_decimal * (days / 30)
            else:
                actual_rate = monthly_rate_decimal

            if method == 'simple':
                # Simple Interest: Interest calculated on original principal for the actual period
                if days < 30:
                    # For loans less than 30 days, calculate interest for exact days
                    total_interest = principal * actual_rate
                    total_payments = principal + total_interest
                    monthly_payment = total_payments  # Single payment for sub-30 day loans
                else:
                    # For longer loans, calculate monthly interest amount
                    monthly_interest_amount = principal * monthly_rate_decimal
                    total_interest = monthly_interest_amount * total_months
                    total_payments = principal + total_interest
                    monthly_payment = total_payments / total_months

                return {
                    'status': True,
                    'monthly_payment': round(monthly_payment, 2),
                    'monthly_rate': monthly_rate_decimal * 100,  # Convert to percentage for display
                    'months': total_months,
                    'total_interest': round(total_interest, 2),
                    'method': 'simple',
                    'actual_days': days
                }

            else:
                # Amortisation using monthly rate
                if days < 30:
                    # For very short loans, use simple interest approach
                    total_interest = principal * actual_rate
                    total_payments = principal + total_interest
                    monthly_payment = total_payments
                else:
                    # Standard amortization formula
                    if monthly_rate_decimal == 0:
                        payment = principal / total_months
                        total_interest = 0
                    else:
                        payment = principal * (monthly_rate_decimal * (1 + monthly_rate_decimal) ** total_months) / (
                                (1 + monthly_rate_decimal) ** total_months - 1)
                        total_payments = payment * total_months
                        total_interest = total_payments - principal
                        monthly_payment = payment

                return {
                    'status': True,
                    'monthly_payment': round(monthly_payment, 2),
                    'monthly_rate': monthly_rate_decimal * 100,  # Convert to percentage for display
                    'months': total_months,
                    'total_interest': round(total_interest, 2),
                    'method': 'amortisation',
                    'actual_days': days
                }

        except ValueError as ve:
            print(f'ValueError: {ve}')
            return {
                'status': False,
                'message': f'Invalid parameter: {str(ve)}'
            }
        except Exception as e:
            print(f'Exception: {e}')
            return {
                'status': False,
                'message': str(e)
            }

    def store_effective_rate(self, loan_id, principal, days, method='amortisation'):
        """
        Calculates and stores the monthly rate and projected total interest amount
        for a given loan in the database using the specified method.
        """
        try:
            # Convert parameters to appropriate types
            if isinstance(principal, str):
                principal = float(principal)
            elif not isinstance(principal, (int, float)):
                raise ValueError(f"Principal parameter must be a number, got {type(principal)}")

            if isinstance(days, str):
                days = float(days)
            elif not isinstance(days, (int, float)):
                raise ValueError(f"Days parameter must be a number, got {type(days)}")

            result = self.determine_monthly_payment(principal, days, method=method)

            if not result['status']:
                return {
                    'status': False,
                    'message': 'Could not determine monthly payment',
                    'error': result.get('message')
                }

            data = {
                'loan_id': loan_id,
                'effective_interest': result['monthly_rate'],  # This is the monthly rate used
                'effective_amount': result['total_interest'],  # Projected total interest
                'method': result['method']
            }

            response = self.supabase.table('effective_rate_amount').insert(data).execute()

            if response.data:
                return {
                    'status': True,
                    'message': 'Monthly rate and projected interest stored successfully',
                    'data': response.data
                }
            else:
                return {
                    'status': False,
                    'message': 'No data returned from Supabase after insert'
                }

        except ValueError as ve:
            print(f"ValueError: {ve}")
            return {
                'status': False,
                'message': f"Invalid parameter: {str(ve)}"
            }
        except Exception as e:
            print(f"Exception: {e}")
            return {
                'status': False,
                'message': f"An error occurred while storing rate information: {str(e)}"
            }

    def loan_estimate_summary(self, principal, days, method):
        """Returns a summarized dictionary of estimated loan information using monthly rate."""
        try:
            # Convert parameters to appropriate types
            if isinstance(principal, str):
                principal = float(principal)
            elif not isinstance(principal, (int, float)):
                raise ValueError(f"Principal parameter must be a number, got {type(principal)}")

            if isinstance(days, str):
                days = float(days)
            elif not isinstance(days, (int, float)):
                raise ValueError(f"Days parameter must be a number, got {type(days)}")

            # Call the monthly payment calculation method
            estimate = self.determine_monthly_payment(principal, days, method)

            if not estimate['status']:
                return {
                    'status': False,
                    'message': 'Could not determine monthly payment',
                    'details': estimate.get('message')
                }

            monthly_payment = estimate['monthly_payment']
            months = estimate['months']
            monthly_rate = estimate['monthly_rate']  # Already in percentage
            total_interest = estimate['total_interest']
            recoverable_amount = monthly_payment * months

            # Calculate monthly interest information based on method
            if method == 'simple':
                # For simple interest: same interest amount each month
                monthly_interest_amount = total_interest / months

            else:
                # For amortisation: interest amount varies each month, so we show average
                monthly_interest_amount = total_interest / months

            # Calculate effective rate based on total payments vs principal
            # This gives us the total return percentage over the loan period
            effective_rate = (total_interest / principal) * 100

            return {
                'status': True,
                'principal': round(principal, 2),
                'recoverable_amount': round(recoverable_amount, 2),
                'monthly_interest_amount': round(monthly_interest_amount, 2),  # Average monthly interest in money
                'monthly_interest_rate': round(monthly_rate, 4),  # Monthly rate as percentage (from database)
                'effective_rate': round(effective_rate, 2),  # Total effective rate over entire period
                'loan_tenure_days': int(days),
                'loan_tenure_months': months,
                'method': method,
                'instalments': round(monthly_payment, 2),  # Monthly payment amount
                'effective_amount': round(total_interest, 2)  # Total interest earned
            }

        except ValueError as ve:
            print(f"ValueError: {ve}")
            return {
                'status': False,
                'message': f'Invalid parameter: {str(ve)}'
            }
        except Exception as e:
            print(f"Exception: {e}")
            return {
                'status': False,
                'message': str(e)
            }

    def generate_payment_schedule_dataframe(self, principal, days, method='amortisation', start_date=None,
                                            file_path=None):
        """
        Generates a detailed payment schedule dataframe based on the monthly rate and loan method.

        Args:
            principal: Loan amount
            days: Loan duration in days
            method: 'simple' or 'amortisation'
            start_date: Start date for the loan (defaults to today)
            file_path: Where to save the CSV (defaults to current directory)

        Returns:
            dict: Status and file path information
        """
        try:
            # Convert parameters to appropriate types
            if isinstance(principal, str):
                principal = float(principal)
            elif not isinstance(principal, (int, float)):
                raise ValueError(f"Principal parameter must be a number, got {type(principal)}")

            if isinstance(days, str):
                days = float(days)
            elif not isinstance(days, (int, float)):
                raise ValueError(f"Days parameter must be a number, got {type(days)}")

            # Get payment calculation details
            estimate = self.determine_monthly_payment(principal, days, method)

            if not estimate['status']:
                return {
                    'status': False,
                    'message': 'Could not determine monthly payment',
                    'details': estimate.get('message')
                }

            monthly_payment = estimate['monthly_payment']
            months = estimate['months']
            monthly_rate_percent = estimate['monthly_rate']  # Already converted to percentage
            monthly_rate_decimal = monthly_rate_percent / 100  # Convert back to decimal for calculations

            # Set start date
            if start_date is None:
                start_date = datetime.now()
            elif isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')

            # Prepare schedule data
            schedule_data = []
            remaining_balance = principal

            if method == 'simple':
                # Simple Interest: Interest is calculated on original principal each month
                monthly_interest_amount = principal * monthly_rate_decimal
                monthly_principal_payment = monthly_payment - monthly_interest_amount

                for month in range(1, months + 1):
                    payment_date = start_date + timedelta(days=30 * month)

                    # For simple interest, adjust last payment to clear remaining balance
                    if month == months:
                        principal_payment = remaining_balance
                        interest_payment = monthly_payment - principal_payment
                    else:
                        principal_payment = monthly_principal_payment
                        interest_payment = monthly_interest_amount

                    remaining_balance -= principal_payment

                    schedule_data.append({
                        'Payment_Number': month,
                        'Payment_Date': payment_date.strftime('%Y-%m-%d'),
                        'Beginning_Balance': round(remaining_balance + principal_payment, 2),
                        'Monthly_Payment': round(monthly_payment, 2),
                        'Interest_Payment': round(interest_payment, 2),
                        'Principal_Payment': round(principal_payment, 2),
                        'Ending_Balance': round(max(0, remaining_balance), 2),
                        'Interest_Rate_Applied': round(monthly_rate_decimal * 100, 4)
                    })

            else:
                # Amortisation: Interest calculated on remaining balance
                for month in range(1, months + 1):
                    payment_date = start_date + timedelta(days=30 * month)

                    # Calculate interest on remaining balance
                    interest_payment = remaining_balance * monthly_rate_decimal
                    principal_payment = monthly_payment - interest_payment

                    # Adjust last payment to clear any remaining balance due to rounding
                    if month == months:
                        principal_payment = remaining_balance
                        interest_payment = monthly_payment - principal_payment

                    remaining_balance -= principal_payment

                    schedule_data.append({
                        'Payment_Number': month,
                        'Payment_Date': payment_date.strftime('%Y-%m-%d'),
                        'Beginning_Balance': round(remaining_balance + principal_payment, 2),
                        'Monthly_Payment': round(monthly_payment, 2),
                        'Interest_Payment': round(interest_payment, 2),
                        'Principal_Payment': round(principal_payment, 2),
                        'Ending_Balance': round(max(0, remaining_balance), 2),
                        'Interest_Rate_Applied': round(monthly_rate_decimal * 100, 4)
                    })

            # Create DataFrame
            df = pd.DataFrame(schedule_data)

            # Calculate effective rate from the schedule
            total_payments = df['Monthly_Payment'].sum()
            total_interest = df['Interest_Payment'].sum()
            effective_rate = (total_interest / principal) * 100

            # Add summary rows at the end
            totals_row = {
                'Payment_Number': 'TOTALS',
                'Payment_Date': '',
                'Beginning_Balance': '',
                'Monthly_Payment': df['Monthly_Payment'].sum(),
                'Interest_Payment': df['Interest_Payment'].sum(),
                'Principal_Payment': df['Principal_Payment'].sum(),
                'Ending_Balance': '',
                'Interest_Rate_Applied': ''
            }

            # Add summary information
            summary_info = [
                {'Payment_Number': '', 'Payment_Date': '', 'Beginning_Balance': '', 'Monthly_Payment': '',
                 'Interest_Payment': '', 'Principal_Payment': '', 'Ending_Balance': '', 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'LOAN SUMMARY', 'Payment_Date': '', 'Beginning_Balance': '', 'Monthly_Payment': '',
                 'Interest_Payment': '', 'Principal_Payment': '', 'Ending_Balance': '', 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'Original Principal', 'Payment_Date': f'K{principal:,.2f}', 'Beginning_Balance': '',
                 'Monthly_Payment': '', 'Interest_Payment': '', 'Principal_Payment': '', 'Ending_Balance': '',
                 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'Total Interest', 'Payment_Date': f'K{df["Interest_Payment"].sum():,.2f}',
                 'Beginning_Balance': '', 'Monthly_Payment': '', 'Interest_Payment': '', 'Principal_Payment': '',
                 'Ending_Balance': '', 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'Total Payments', 'Payment_Date': f'K{df["Monthly_Payment"].sum():,.2f}',
                 'Beginning_Balance': '', 'Monthly_Payment': '', 'Interest_Payment': '', 'Principal_Payment': '',
                 'Ending_Balance': '', 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'Monthly Rate', 'Payment_Date': f'{monthly_rate_percent}%', 'Beginning_Balance': '',
                 'Monthly_Payment': '', 'Interest_Payment': '', 'Principal_Payment': '', 'Ending_Balance': '',
                 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'Effective Rate', 'Payment_Date': f'{effective_rate:.2f}%', 'Beginning_Balance': '',
                 'Monthly_Payment': '', 'Interest_Payment': '', 'Principal_Payment': '', 'Ending_Balance': '',
                 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'Loan Method', 'Payment_Date': method.title(), 'Beginning_Balance': '',
                 'Monthly_Payment': '', 'Interest_Payment': '', 'Principal_Payment': '', 'Ending_Balance': '',
                 'Interest_Rate_Applied': ''},
                {'Payment_Number': 'Loan Duration', 'Payment_Date': f'{int(days)} days ({months} months)',
                 'Beginning_Balance': '', 'Monthly_Payment': '', 'Interest_Payment': '', 'Principal_Payment': '',
                 'Ending_Balance': '', 'Interest_Rate_Applied': ''}
            ]

            # Add totals and summary to DataFrame
            df = pd.concat([df, pd.DataFrame([totals_row]), pd.DataFrame(summary_info)], ignore_index=True)

            # Generate file path if not provided
            if file_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                file_path = f'payment_schedule_{method}_{timestamp}.csv'

            return {
                'status': True,
                'message': 'Payment schedule generated successfully',
                'schedule_dataframe': df,  # The actual pandas DataFrame
                'csv_filename': file_path,  # Suggested filename
                'total_payments': len(schedule_data),
                'total_interest': round(df.iloc[:-9]['Interest_Payment'].sum(), 2),
                'total_amount': round(df.iloc[:-9]['Monthly_Payment'].sum(), 2),
                'monthly_rate': monthly_rate_percent,
                'effective_rate': round(effective_rate, 2)
            }

        except ValueError as ve:
            print(f"ValueError: {ve}")
            return {
                'status': False,
                'message': f'Invalid parameter: {str(ve)}'
            }
        except Exception as e:
            print(f"Exception: {e}")
            return {
                'status': False,
                'message': f'An error occurred while generating payment schedule: {str(e)}'
            }

    def generate_loan_contract(self, borrower_name, borrower_id, organisation_name, principal, days, method):
        """Generates a contract for that loan and returns the contract content"""
        try:
            # Get loan summary
            loan_summary = self.loan_estimate_summary(principal, days, method)

            if not loan_summary['status']:
                return {
                    'status': False,
                    'message': 'Could not generate loan summary',
                    'contract_content': None
                }

            # Read the base template
            try:
                with open('templates/contract.txt', 'r') as file:
                    template = file.read()
            except FileNotFoundError:
                return {
                    'status': False,
                    'message': 'Contract template file not found',
                    'contract_content': None
                }

            # Replace placeholders
            filled_contract = template.format(
                borrower_name=borrower_name,
                borrower_id=borrower_id,
                organisation_name=organisation_name,
                principal=loan_summary['principal'],
                recoverable_amount=loan_summary['recoverable_amount'],
                monthly_interest_rate=loan_summary['monthly_interest_rate'],
                loan_tenure_days=loan_summary['loan_tenure_days'],
                loan_tenure_months=loan_summary.get('loan_tenure_months', round(int(days) / 30)),
                method=loan_summary['method'],
                instalments=loan_summary['instalments']
            )

            # Optionally save the filled contract to a file (for backup)
            output_path = 'templates/generated_contract.txt'
            try:
                with open(output_path, 'w') as file:
                    file.write(filled_contract)
            except Exception as e:
                print(f"Warning: Could not save contract to file: {e}")

            return {
                'status': True,
                'message': 'Contract generated successfully',
                'contract_content': filled_contract,
                'file_path': output_path
            }

        except Exception as e:
            print(f"Exception in generate_loan_contract: {e}")
            return {
                'status': False,
                'message': f'Error generating contract: {str(e)}',
                'contract_content': None
            }



test = Loans()

print(
    test.verify_borrower('123456/12/1','ee8278fc-66e6-4a0f-a6bb-3525657f98b8')
)