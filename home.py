import textwrap

import bcrypt
from supabase import create_client, Client
from flask import session
import os
import random
import string
import smtplib
from email.message import EmailMessage

from datetime import datetime, timedelta
import pandas as pd


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

    def interest_per_quarter(self, year):
        """returns interest sum per quarter for the year chosen"""

        try:
            # Fetch interest_component and payment_date for the chosen year
            response = (
                self.supabase
                .table('loan_repayments')
                .select('interest_component, created_at')
                .gte('created_at', f'{year}-01-01')
                .lte('created_at', f'{year}-12-31')
                .execute()
            )

            if not response.data:
                return {
                    "first quarter": 0,
                    "second quarter": 0,
                    "third quarter": 0,
                    "fourth quarter": 0
                }

            # Initialize quarter totals
            quarters = {
                "first quarter": 0,
                "second quarter": 0,
                "third quarter": 0,
                "fourth quarter": 0
            }

            for row in response.data:
                interest = float(row['interest_component'])
                month = int(row['payment_date'][5:7])  # extract month from 'YYYY-MM-DD'

                if month in (1, 2, 3):
                    quarters["first quarter"] += interest
                elif month in (4, 5, 6):
                    quarters["second quarter"] += interest
                elif month in (7, 8, 9):
                    quarters["third quarter"] += interest
                elif month in (10, 11, 12):
                    quarters["fourth quarter"] += interest

            return quarters

        except Exception as e:
            print(f"Exception while fetching interest per quarter: {e}")
            return None

    def get_nominal_rate(self):
        """returns the nominal rate for the business"""
        try:
            response = self.supabase.table('nominal_rate').select('nominal_rate').execute()
            return response.data[0]['nominal_rate']

        except Exception as e:
            print(f"Exception while fetching interest per quarter: {e}")
            return None

    def total_principal_repaid(self):
        """Returns the sum of principal repaid from loan_repayments"""
        try:
            response = (
                self.supabase
                .table('loan_repayments')
                .select('principal_component')
                .execute()
            )

            total = sum(float(row['principal_component']) for row in response.data)
            return total

        except Exception as e:
            print(f"Error calculating principal sum: {e}")
            return None

    def total_loan_disbursed(self):
        """returns the sum of total loans disbursed"""
        try:
            response = (
                self.supabase
                .table('loans')
                .select('loan_amount')
                .execute()
            )
            if not response.data:
                return 0

            total = sum(float(row['loan_amount']) for row in response.data)
            return total

        except Exception as e:
            print(f"Error calculating total loan disbursed: {e}")
            return 0

    def get_repayment_summary_all(self):
        """Returns full repayment schedules for all loans with optimized loan request fetching."""
        try:
            # Fetch all loans
            loans_response = self.supabase.table('loans').select('*').execute()
            loans = loans_response.data

            # Fetch all loan_requests once
            loan_requests_response = self.supabase.table('loan_requests').select('id, method').execute()
            loan_requests = loan_requests_response.data

            # Create a dictionary mapping loan_request_id to method
            loan_request_map = {lr['id']: lr['method'].lower() for lr in loan_requests}

            all_rows = []
            columns = ["Loan ID", "No", "Due Date", "Payment Due", "Interest", "Principal", "Balance", "Actual Paid",
                       "Status"]

            for loan in loans:
                loan_type = loan_request_map.get(loan['loan_request_id'], 'amortization')  # default to amortization

                agreed_amount = float(loan['monthly_payment'])
                original_balance = float(loan['loan_amount'])
                annual_rate = float(loan['interest_rate']) / 100
                monthly_rate = annual_rate / 12

                current_date = datetime.strptime(loan['start_date'], "%Y-%m-%d")
                remaining_balance = original_balance

                repayment_query = self.supabase.table('loan_repayments').select('*').eq('loan_id', loan['id'])

                for number in range(1, loan['term_months'] + 1):
                    next_date = current_date + timedelta(days=30)

                    repayments_this_month = (
                        repayment_query
                        .gte('created_at', current_date.strftime("%Y-%m-%d"))
                        .lt('created_at', next_date.strftime("%Y-%m-%d"))
                        .execute()
                    )
                    payments_data = repayments_this_month.data
                    amount_paid_this_month = sum(float(r['payment_amount']) for r in payments_data)

                    if payments_data:
                        latest_payment = sorted(payments_data, key=lambda x: x['payment_date'], reverse=True)[0]
                        interest = float(latest_payment['interest_amount'])
                        principal = float(latest_payment['principal_amount'])
                        balance = float(latest_payment['balance'])
                    else:
                        if loan_type == "simple":
                            interest = original_balance * monthly_rate
                            principal = agreed_amount - interest
                        elif loan_type == "amortization":
                            interest = remaining_balance * monthly_rate
                            principal = agreed_amount - interest
                        else:
                            interest = 0
                            principal = agreed_amount

                        balance = max(0, remaining_balance - principal)

                    payment_due = agreed_amount - amount_paid_this_month
                    status = "Paid" if payment_due <= 0 else "Pending"

                    all_rows.append((
                        loan['id'],
                        number,
                        next_date.strftime("%Y-%m-%d"),
                        round(payment_due, 2),
                        round(interest, 2),
                        round(principal, 2),
                        round(balance, 2),
                        round(amount_paid_this_month, 2),
                        status
                    ))

                    current_date = next_date
                    remaining_balance = balance

            return pd.DataFrame(all_rows, columns=columns)

        except Exception as e:
            print(f"Error generating repayment schedules: {e}")
            return None

    def total_interest_paid(self):
        try:
            response = (
                self.supabase
                .table('loan_repayments')
                .select('interest_component')
                .execute()
            )
            total_paid = sum(float(row['interest_component']) for row in response.data)
            return total_paid
        except Exception as e:
            print(f"Error calculating interest paid: {e}")
            return 0

    def consolidated_ammortised_table(self):
        """returns a consolidated dataframe """
        import pandas as pd
        from datetime import datetime, timedelta

        # query the loans table to get the loan_amount field value which is the principal value, the interest_rate value
        # which is in decimal,the term_months to get the tenure, and the monthly_payment to get how much will be paid per month
        loan_response = (
            self.supabase
            .table('loans')
            .select('*')
            .execute()
        )

        loans = loan_response.data

        # using the loan_request_id from loans table query the loan_requests table to get the value in the method field which either be simple
        # for simple interest or amortisation. this will be used to calculate what part of the monthly_payment is the interest_component and the pincipal_component
        loan_request_ids = [loan['loan_request_id'] for loan in loans]

        loan_requests_response = (
            self.supabase
            .table('loan_requests')
            .select('id, method')
            .in_('id', loan_request_ids)
            .execute()
        )

        loan_requests = {req['id']: req['method'] for req in loan_requests_response.data}

        # Query loan_repayments table to check payment status for each loan
        loan_ids = [loan['id'] for loan in loans]

        repayments_response = (
            self.supabase
            .table('loan_repayments')
            .select('loan_id, created_at, payment_amount')
            .in_('loan_id', loan_ids)
            .order('loan_id, created_at')
            .execute()
        )

        # Group repayments by loan_id and count them
        loan_repayments = {}
        for repayment in repayments_response.data:
            loan_id = repayment['loan_id']
            if loan_id not in loan_repayments:
                loan_repayments[loan_id] = 0
            loan_repayments[loan_id] += 1

        # create a data frame querying each loan in the loans table and making rows for all the payments calculating the interest_component and the
        # principal component based on the method that are supposed to take place for that loan
        # when done with all the rows for that loan immediately input the next rows for the next loan right after the last row of the current loan
        # do not calculate totals we are just making entries

        all_payment_rows = []

        for loan in loans:
            loan_id = loan.get('loan_id')
            principal = float(loan['loan_amount'])
            annual_rate = float(loan['interest_rate'])
            term_months = int(loan['term_months'])
            monthly_payment = float(loan['monthly_payment'])
            loan_request_id = loan['loan_request_id']

            # Get the method from loan_requests
            method = loan_requests.get(loan_request_id, 'amortisation')  # default to amortisation

            # Get number of repayments made for this loan
            repayments_made = loan_repayments.get(loan_id, 0)

            remaining_balance = principal

            for payment_num in range(1, term_months + 1):
                if method.lower() == 'simple':
                    # Simple interest calculation
                    # For simple interest, interest component remains constant each month
                    total_interest = principal * annual_rate * (term_months / 12)
                    interest_component = total_interest / term_months
                    principal_component = monthly_payment - interest_component

                    # Update remaining balance
                    remaining_balance = max(0, remaining_balance - principal_component)

                else:  # amortisation method
                    # Amortizing loan calculation
                    monthly_rate = annual_rate / 12

                    if remaining_balance > 0:
                        # Calculate interest component for this month
                        interest_component = remaining_balance * monthly_rate

                        # Calculate principal component for this month
                        principal_component = monthly_payment - interest_component

                        # Ensure principal component doesn't exceed remaining balance
                        if principal_component > remaining_balance:
                            principal_component = remaining_balance
                            interest_component = monthly_payment - principal_component

                        # Update remaining balance
                        remaining_balance = max(0, remaining_balance - principal_component)
                    else:
                        # If balance is already zero
                        interest_component = 0
                        principal_component = 0

                # Check if this payment has been made by comparing payment number with actual repayments
                is_paid = payment_num <= repayments_made

                # Ensure loan_created_date is a datetime object
                loan_created_date = loan['created_at']
                if isinstance(loan_created_date, str):
                    loan_created_date = datetime.fromisoformat(loan_created_date)

                current_date = datetime.today()
                due_date = loan_created_date + timedelta(days=30 * payment_num)

                is_due = current_date >= due_date


                # Create row for this payment
                payment_row = {
                    'loan_id': loan_id,
                    'payment_number': payment_num,
                    'monthly_payment': monthly_payment,
                    'interest_component': round(interest_component, 2),
                    'principal_component': round(principal_component, 2),
                    'remaining_balance': round(remaining_balance, 2),
                    'method': method,
                    'paid': is_paid,
                    'due_date': due_date,
                    'due': is_due,
                }

                all_payment_rows.append(payment_row)


        # Create consolidated DataFrame
        consolidated_df = pd.DataFrame(all_payment_rows)

        return consolidated_df

    def expected_interest(self):
        """returns the total amount of false paid columns in the interest_component of the dataframe"""
        dataframe = self.consolidated_ammortised_table()

        # Filter rows where paid is False (unpaid payments)
        unpaid_payments = dataframe[dataframe['paid'] == False]

        # Sum the interest_component for all unpaid payments
        total_expected_interest = unpaid_payments['interest_component'].sum()

        return round(total_expected_interest, 2)

    def total_receivables(self):
        """returns the total amount of false paid columns in the interest_component of the dataframe"""
        dataframe = self.consolidated_ammortised_table()

        # Filter rows where due is True (due payments)
        due_payments = dataframe[dataframe['due'] == True]

        # Sum the interest_component for all due payments
        total_receivables = due_payments['monthly_payment'].sum()

        return round(total_receivables, 2)


test = Home()
print(test.interest_per_quarter(2025))