from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
from dotenv import load_dotenv
from flask_wtf.csrf import CSRFProtect, generate_csrf

import os
from datetime import datetime
import traceback
import secrets
import io
import pandas as pd

# Load environment variables
load_dotenv()

# modules
from auth import UserAuthentication
from home import Home
from loans import Loans
from organisations import Organisations
from borrowers import Borrowers

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY') or 'fallback-secret-key-for-development'
csrf = CSRFProtect(app)


# Make CSRF token available in all templates
@app.context_processor
def inject_csrf_token():
    return dict(csrf_token=generate_csrf())


# Add a root route
@app.route('/')
def index():
    return redirect(url_for('login'))


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        user_name = request.form.get('user_name')
        email = request.form.get('email')
        nrc_number = request.form.get('nrc_number')
        date_of_birth = request.form.get('date_of_birth')
        user_type = request.form.get('user_type')
        password = request.form.get('password')
        secret_key = request.form.get('secret_key')

        # Basic input validation
        if not all([user_name, email, nrc_number, date_of_birth, user_type, password, secret_key]):
            flash('All fields are required.', 'error')
            return redirect(url_for('signup'))

        try:
            auth_manager = UserAuthentication()
            response = auth_manager.sign_up(user_name, email, nrc_number, date_of_birth, user_type, password,
                                            secret_key)

            if response.get('error'):
                flash(response['error'], 'error')
                return redirect(url_for('signup'))

            flash('Signup successful!', 'success')
            return redirect(url_for('login'))

        except Exception as e:
            print(f'Exception: {e}')
            flash('An unexpected error occurred. Please try again.', 'error')
            return redirect(url_for('signup'))

    # GET request: render signup form
    return render_template('auth.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('auth.html')

    # POST request
    email = request.form.get('email')
    password = request.form.get('password')

    # Basic validation
    if not email or not password:
        flash('Email and password are required.', 'error')
        return redirect(url_for('login'))

    try:
        auth_manager = UserAuthentication()
        response = auth_manager.login(email, password)

        if response.get('error'):
            flash(response['error'], 'error')
            return redirect(url_for('login'))

        # Store user info in session
        session['user_type'] = response['user_type']
        session['email'] = email

        flash('Login successful!', 'success')
        return redirect(url_for('home'))

    except Exception as e:
        print(f'Exception: {e}')
        flash('An unexpected error occurred. Please try again.', 'error')
        return redirect(url_for('login'))


@app.route('/home')
def home():
    # Check if user is logged in
    if 'email' not in session:
        flash('Please log in to access this page.', 'error')
        return redirect(url_for('login'))

    home_manager = Home()
    total_principal_given = home_manager.total_principal_given()
    interest_earned = home_manager.interest_earned()
    total_receivables = home_manager.total_receivables()

    return render_template('home.html',
                           total_principal_given=total_principal_given,
                           interest_earned=interest_earned,
                           total_receivables=total_receivables
                           )


@app.route('/organisation_transactions', methods=['POST','GET'])
def organisation_transactions():

    loans_manager = Loans()
    organisation_data = loans_manager.organisation_summary()
    organisation = loans_manager.organisation_revenue_and_balance(None)

    return render_template('organisation_transactions.html',
                           organisation = organisation,
                           organisation_data = organisation_data
                           )


@app.route('/organisation_borrowers/<org_id>', methods=['GET', 'POST'])
def organisation_borrowers(org_id):
    # Now you can use org_id to call your method
    loans_manager = Loans()
    organisations_manager = Organisations()


    individuals = loans_manager.active_organisational_borrowers(org_id)
    funds_collected = loans_manager.organisation_revenue_and_balance(organisational_id=org_id)
    fund_balance = loans_manager.organisation_revenue_and_balance(organisational_id=org_id)
    org_names = organisations_manager.get_organisations()
    organisation_name = organisations_manager.get_organisational_name(org_id)
    loans = loans_manager.organisations_loans(org_id)

    # Your other logic here
    return render_template('organisation_borrowers.html',
                           organisation_name=organisation_name,
                           org_id=org_id,
                           org_names = org_names,
                           individuals = individuals,
                           funds_collected = funds_collected.get('total_revenue'),
                           fund_balance =fund_balance.get('total_balance'),
                           loans = loans)


@app.route('/borrower_management')
def borrower_management():
    organisation_manager = Organisations()
    organisations = organisation_manager.get_organisations()
    borrowers_manager = Borrowers()
    borrowers = borrowers_manager.exhaust_borrower_information()

    return render_template('borrowers.html',
                           organisations=organisations,
                           borrowers=borrowers
                           )


@app.route('/add_borrower', methods=['POST', 'GET'])
def add_borrower():
    organisation_manager = Organisations()
    organisations = organisation_manager.get_organisations()
    borrowers_manager = Borrowers()

    if request.method == 'POST':
        try:
            print("=== POST REQUEST DEBUG INFO ===")
            print(f"Form data keys: {list(request.form.keys())}")
            print(f"Files data keys: {list(request.files.keys())}")
            print(f"Total files from form: {request.form.get('total_files', 'Not set')}")

            # Debug: Print all form data
            for key, value in request.form.items():
                print(f"Form field - {key}: {value}")

            # Debug: Print all files
            for key, file in request.files.items():
                if file and file.filename:
                    print(f"File - {key}: {file.filename} ({file.content_type}) - {file.content_length} bytes")
                else:
                    print(f"File - {key}: Empty or invalid file")

            # Handle the borrower creation with files
            result = borrowers_manager.create_borrower_with_files(
                form_data=request.form,
                request_files=request.files
            )

            print(f"Final result from create_borrower_with_files: {result}")

            if result["success"]:
                response_data = {
                    "success": True,
                    "message": "Borrower created successfully",
                    "borrower_id": result["borrower_id"],
                    "files_processed": result.get("file_upload_result", {}).get("files_processed", 0),
                    "file_urls": result.get("file_upload_result", {}).get("file_urls", {}),
                    "database_result": result.get("file_upload_result", {}).get("database_result", {})
                }
                print(f"Sending success response: {response_data}")
                return jsonify(response_data), 200
            else:
                error_response = {
                    "success": False,
                    "message": result["message"],
                    "error": result.get("error", "Unknown error"),
                    "details": result
                }
                print(f"Sending error response: {error_response}")
                return jsonify(error_response), 400

        except Exception as e:
            error_msg = f"Server error: {str(e)}"
            print(f"Exception in route: {error_msg}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")

            return jsonify({
                "success": False,
                "message": error_msg,
                "error": str(e)
            }), 500

    # GET request - render the page
    try:
        borrowers = borrowers_manager.exhaust_borrower_information()

        return render_template('borrowers.html',
                               borrowers=borrowers,
                               organisations=organisations)
    except Exception as e:
        error_msg = f"Error loading page: {str(e)}"
        print(error_msg)
        return error_msg, 500


# GET route to fetch borrower data for editing
@app.route('/get_borrower_data/<borrower_id>', methods=['GET'])
def get_borrower_data(borrower_id):
    """Fetch borrower data for populating the edit form"""
    borrowers = Borrowers()
    result = borrowers.get_borrower_edit_data(borrower_id)

    if result["success"]:
        return jsonify(result["borrower_data"])
    else:
        return jsonify({"error": result["error"]}), 400


# POST route to handle borrower updates
@app.route('/update_borrower', methods=['POST'])
def update_borrower():
    """Handle borrower update requests"""
    borrowers_manager = Borrowers()

    try:
        borrower_id = request.form.get('borrower_id')

        if not borrower_id:
            return jsonify({
                "success": False,
                "error": "Borrower ID is required",
                "message": "Cannot update borrower without ID"
            }), 400

        print("=== UPDATE REQUEST DEBUG INFO ===")
        print(f"Borrower ID: {borrower_id}")
        print(f"Form data keys: {list(request.form.keys())}")
        print(f"Files data keys: {list(request.files.keys())}")
        print(f"Total files from form: {request.form.get('total_files', 'Not set')}")

        # Debug: Print all form data
        for key, value in request.form.items():
            print(f"Form field - {key}: {value}")

        # Debug: Print all files
        for key, file in request.files.items():
            if file and file.filename:
                print(f"File - {key}: {file.filename} ({file.content_type}) - {file.content_length} bytes")
            else:
                print(f"File - {key}: Empty or invalid file")

        # Handle the borrower update with files
        # You'll need to create this method in your Borrowers class
        result = borrowers_manager.update_borrower_with_files(
            borrower_id=borrower_id,
            form_data=request.form,
            request_files=request.files
        )

        print(f"Final result from update_borrower_with_files: {result}")

        if result["success"]:
            response_data = {
                "success": True,
                "message": "Borrower updated successfully",
                "borrower_id": borrower_id,
                "files_processed": result.get("file_upload_result", {}).get("files_processed", 0),
                "file_urls": result.get("file_upload_result", {}).get("file_urls", {}),
                "database_result": result.get("file_upload_result", {}).get("database_result", {})
            }
            print(f"Sending success response: {response_data}")
            return jsonify(response_data), 200
        else:
            error_response = {
                "success": False,
                "message": result["message"],
                "error": result.get("error", "Unknown error"),
                "details": result
            }
            print(f"Sending error response: {error_response}")
            return jsonify(error_response), 400

    except Exception as e:
        error_msg = f"Server error during update: {str(e)}"
        print(f"Exception in update route: {error_msg}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

        return jsonify({
            "success": False,
            "message": error_msg,
            "error": str(e)
        }), 500


@app.route('/loan_information/<loan_id>', methods=['POST','GET'])
def loan_information(loan_id):

    loan_manager = Loans()
    borrower = loan_manager.get_borrower_by_loan(loan_id)
    loan = loan_manager.get_loan_info_by_id(loan_id)

    return render_template('loan_information.html',
                           borrower = borrower,
                           loan = loan
                           )


@app.route('/loan_application', methods=['POST','GET'])
def loan_application():
    organisation_manager = Organisations()
    organisations = organisation_manager.get_organisations()

    return render_template('loan_application.html',
                           organisations = organisations)


@app.route('/loan_application_summary', methods=['POST', 'GET'])
def loan_application_summary():
    loan_manager = Loans()
    organisation_manager = Organisations()

    organisation_id = request.form.get('organisation_id')
    nrc_number = request.form.get('borrower_nrc')

    verified = loan_manager.verify_borrower(nrc_number=nrc_number,
                                            organisation_id=organisation_id
                                            )
    if not verified.get('status'):
        flash('Organisation and Nrc number do not match')
        return redirect(url_for('loan_application'))  # Redirect back to form

    principal = request.form.get('principal')
    days = request.form.get('days')
    method = request.form.get('method')

    estimate_summary = loan_manager.loan_estimate_summary(principal, days, method)
    print(estimate_summary)

    if not estimate_summary:
        flash('something went wrong')
        return redirect(url_for('loan_application'))  # Redirect back to form

    # Generate payment schedule
    payment_schedule_result = loan_manager.generate_payment_schedule_dataframe(principal, days, method)

    # Convert DataFrame to JSON for JavaScript
    schedule_data = None
    if payment_schedule_result['status']:
        df = payment_schedule_result['schedule_dataframe']
        # Convert DataFrame to list of dictionaries for easier JavaScript handling
        schedule_data = df.to_dict('records')

    # Generate loan contract
    loan_contract_result = loan_manager.generate_loan_contract(
        borrower_name=f"{verified['data'][0]['first_name']} {verified['data'][0]['last_name']}",
        borrower_id=f"{verified['data'][0]['id']}",
        organisation_name=organisation_manager.get_organisational_name(verified['data'][0]['organisation_id']),
        principal=principal,
        days=days,
        method=method
    )

    loan_contract = None
    if loan_contract_result and loan_contract_result['status']:
        loan_contract = loan_contract_result['contract_content']

    return render_template('view_application_summary.html',
                           summary=estimate_summary,
                           schedule_data=schedule_data,
                           loan_contract=loan_contract,
                           borrower_info={
                               'name': f"{verified['data'][0]['first_name']} {verified['data'][0]['last_name']}",
                               'id': verified['data'][0]['id'],
                               'organisation': organisation_manager.get_organisational_name(verified['data'][0]['organisation_id'])
                           },
                           loan_params={
                               'principal': principal,
                               'days': days,
                               'method': method
                           }
                           )







@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out successfully.', 'success')
    return redirect(url_for('login'))




if __name__ == '__main__':
    app.run(debug=True)