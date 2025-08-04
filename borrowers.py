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


def get_content_type(file_extension):
    """Helper function to get content type based on file extension"""
    content_type, _ = mimetypes.guess_type(f"file{file_extension}")
    return content_type or "application/octet-stream"


class Borrowers:
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

    def exhaust_borrower_information(self):
        """returns full exhausted information for a borrowers"""
        try:
            # Step 1: Get all borrowers
            borrowers_response = (
                self.supabase
                .table('borrowers')
                .select('*')
                .execute()
            )

            if not borrowers_response.data:
                return []

            borrowers = borrowers_response.data

            for borrower in borrowers:
                organisation_id = borrower.get('organisation_id')
                borrower_id = borrower.get('id')
                next_of_kin_id = borrower.get('next_of_kin_id')

                # Step 2: Add organisation name
                if organisation_id:
                    try:
                        org_response = (
                            self.supabase
                            .table('organisations')
                            .select('name')
                            .eq('id', organisation_id)
                            .limit(1)
                            .execute()
                        )

                        if org_response.data:
                            borrower['organisation_name'] = org_response.data[0]['name']
                        else:
                            borrower['organisation_name'] = None
                    except Exception as e:
                        print(f"Error fetching organisation for ID {organisation_id}: {e}")
                        borrower['organisation_name'] = None
                else:
                    borrower['organisation_name'] = None

                # Step 3: Add latest loan repayment balance
                if borrower_id:
                    try:
                        repayment_response = (
                            self.supabase
                            .table('loan_repayments')
                            .select('balance, created_at')
                            .eq('borrower_id', borrower_id)
                            .order('created_at', desc=True)
                            .limit(1)
                            .execute()
                        )

                        if repayment_response.data:
                            borrower['latest_balance'] = repayment_response.data[0]['balance']
                        else:
                            borrower['latest_balance'] = None
                    except Exception as e:
                        print(f"Error fetching repayments for borrower ID {borrower_id}: {e}")
                        borrower['latest_balance'] = None

                    # Step 4: Sum of remaining payments for active loans
                    try:
                        loans_response = (
                            self.supabase
                            .table('loans')
                            .select('remaining_payments')
                            .eq('borrower_id', borrower_id)
                            .eq('status', 'active')
                            .execute()
                        )

                        if loans_response.data:
                            total_remaining = sum(
                                loan.get('remaining_payments', 0) or 0
                                for loan in loans_response.data
                            )
                            borrower['total_remaining_payments'] = total_remaining
                        else:
                            borrower['total_remaining_payments'] = 0
                    except Exception as e:
                        print(f"Error fetching loans for borrower ID {borrower_id}: {e}")
                        borrower['total_remaining_payments'] = 0
                else:
                    borrower['latest_balance'] = None
                    borrower['total_remaining_payments'] = 0

                # Step 5: Add next_of_kin info
                if next_of_kin_id:
                    try:
                        nok_response = (
                            self.supabase
                            .table('next_of_kins')
                            .select('first_name, last_name, email, phone')
                            .eq('id', next_of_kin_id)
                            .limit(1)
                            .execute()
                        )

                        if nok_response.data:
                            borrower['next_of_kin'] = nok_response.data[0]
                        else:
                            borrower['next_of_kin'] = None
                    except Exception as e:
                        print(f"Error fetching next of kin for ID {next_of_kin_id}: {e}")
                        borrower['next_of_kin'] = None
                else:
                    borrower['next_of_kin'] = None

            # Step 6: Return enriched borrower data
            return borrowers

        except Exception as e:
            print(f'Exception in exhaust_borrower_information: {e}')
            return []

    def upload_borrower_file(self, file_object, file_name, document_type):
        """
        Upload a file to the borrower-files bucket in Supabase

        Args:
            file_object: File object or file content (bytes)
            file_name: Original filename
            document_type: Type of document ('identity', 'residence', 'photo')

        Returns:
            dict: Contains success status, file URL, and file path
        """
        try:
            # Validate inputs
            if not file_object:
                return {
                    "success": False,
                    "error": "No file provided",
                    "message": "File object is required"
                }

            if not file_name:
                return {
                    "success": False,
                    "error": "No filename provided",
                    "message": "Filename is required"
                }

            if not document_type:
                return {
                    "success": False,
                    "error": "No document type provided",
                    "message": "Document type is required"
                }

            # Generate unique filename to avoid conflicts
            file_extension = os.path.splitext(file_name)[1]
            unique_id = str(uuid.uuid4())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Create file path (all files go to the same bucket without borrower_id organization)
            file_path = f"{document_type}/{timestamp}_{unique_id}{file_extension}"

            # Upload file to bucket
            response = self.supabase.storage.from_("borrower-files").upload(
                path=file_path,
                file=file_object,
                file_options={
                    "content-type": get_content_type(file_extension),
                    "upsert": False  # Don't overwrite existing files
                }
            )

            # Check if upload was successful
            if hasattr(response, 'data') and response.data:
                # Get public URL for the uploaded file
                public_url_response = self.supabase.storage.from_("borrower-files").get_public_url(file_path)

                # Extract the actual URL string from the response
                file_url = public_url_response.get('publicUrl') if hasattr(public_url_response, 'get') else str(
                    public_url_response)

                return {
                    "success": True,
                    "file_url": file_url,
                    "file_path": file_path,
                    "document_type": document_type,
                    "original_filename": file_name,
                    "message": "File uploaded successfully"
                }
            else:
                # Check for error in response
                error_message = "Upload failed"
                if hasattr(response, 'error') and response.error:
                    error_message = f"Upload failed: {response.error}"

                return {
                    "success": False,
                    "error": error_message,
                    "message": "Failed to upload file to storage"
                }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Error uploading file: {str(e)}"
            }

    def upload_multiple_borrower_files(self, files_data):
        """
        Upload multiple files for a borrower

        Args:
            files_data: List of dictionaries containing file info
            Example: [
                {
                    'file_object': file_bytes,
                    'file_name': 'nrc.pdf',
                    'document_type': 'identity',
                    'borrower_id': 'borrower_123'
                },
                ...
            ]

        Returns:
            dict: Contains results for all uploads
        """
        if not files_data or not isinstance(files_data, list):
            return {
                "successful_uploads": [],
                "failed_uploads": [],
                "total_files": 0,
                "success_count": 0,
                "failure_count": 0,
                "message": "No files provided or invalid files_data format"
            }

        results = {
            "successful_uploads": [],
            "failed_uploads": [],
            "total_files": len(files_data)
        }

        for file_data in files_data:
            # Validate file_data structure
            required_keys = ['file_object', 'file_name', 'document_type']
            if not all(key in file_data for key in required_keys):
                results["failed_uploads"].append({
                    "file_name": file_data.get('file_name', 'Unknown'),
                    "error": "Missing required keys in file_data"
                })
                continue

            result = self.upload_borrower_file(
                file_object=file_data['file_object'],
                file_name=file_data['file_name'],
                document_type=file_data['document_type'],
                borrower_id=file_data.get('borrower_id')
            )

            if result["success"]:
                results["successful_uploads"].append(result)
            else:
                results["failed_uploads"].append({
                    "file_name": file_data['file_name'],
                    "error": result["error"],
                    "message": result.get("message", "Upload failed")
                })

        results["success_count"] = len(results["successful_uploads"])
        results["failure_count"] = len(results["failed_uploads"])

        return results

    def process_borrower_form_files(self, form_data, borrower_id):
        """
        Process files from the borrower form submission

        Args:
            form_data: Flask request.files and request.form data
            borrower_id: The borrower ID to associate files with

        Returns:
            dict: Contains upload results and file URLs for database storage
        """
        try:
            # Extract total number of files from form
            total_files = int(form_data.get('total_files', 0))

            if total_files == 0:
                return {
                    "success": True,
                    "message": "No files to process",
                    "file_urls": {
                        "nrc_files": [],
                        "proof_residency_files": []
                    }
                }

            uploaded_results = {
                "successful_uploads": [],
                "failed_uploads": [],
                "file_urls": {
                    "nrc_files": [],
                    "proof_residency_files": []
                }
            }

            # Process each file from the form
            for i in range(total_files):
                file_key = f'file_{i}'
                file_type_key = f'file_{i}_type'
                file_name_key = f'file_{i}_name'

                # Get file data from form
                file_object = form_data.get(file_key)  # This should be from request.files
                document_type = form_data.get(file_type_key)  # This should be from request.form
                original_filename = form_data.get(file_name_key)  # This should be from request.form

                if not file_object or not document_type:
                    uploaded_results["failed_uploads"].append({
                        "file_index": i,
                        "error": "Missing file or document type"
                    })
                    continue

                # Map frontend document types to database field names
                db_document_type = "nrc_files" if document_type == "identity" else "proof_residency_files"

                # Upload file to bucket and get URL
                upload_result = self.upload_borrower_file(
                    file_object=file_object,
                    file_name=original_filename or f"file_{i}",
                    document_type=document_type
                )

                if upload_result["success"]:
                    uploaded_results["successful_uploads"].append(upload_result)
                    # Store URL in appropriate category for database insertion
                    uploaded_results["file_urls"][db_document_type].append(upload_result["file_url"])
                else:
                    uploaded_results["failed_uploads"].append({
                        "file_index": i,
                        "file_name": original_filename,
                        "error": upload_result["error"]
                    })

            uploaded_results["success"] = len(uploaded_results["failed_uploads"]) == 0
            uploaded_results["total_processed"] = total_files
            uploaded_results["success_count"] = len(uploaded_results["successful_uploads"])
            uploaded_results["failure_count"] = len(uploaded_results["failed_uploads"])

            return uploaded_results

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Error processing form files: {str(e)}",
                "file_urls": {
                    "nrc_files": [],
                    "proof_residency_files": []
                }
            }

    def save_borrower_files_to_db(self, borrower_id, file_urls):
        """
        Save file URLs to the borrower_files table

        Args:
            borrower_id: The borrower ID
            file_urls: Dict with nrc_files and proof_residency_files arrays

        Returns:
            dict: Database operation result
        """
        try:
            # Check if borrower_files record exists
            existing_record = (
                self.supabase
                .table('borrower_files')
                .select('id, nrc_files, proof_residency_files')
                .eq('borrower_id', borrower_id)
                .limit(1)
                .execute()
            )

            if existing_record.data:
                # Update existing record by merging arrays
                record_id = existing_record.data[0]['id']
                current_nrc = existing_record.data[0].get('nrc_files', []) or []
                current_residence = existing_record.data[0].get('proof_residency_files', []) or []

                # Merge new files with existing ones
                updated_nrc = current_nrc + file_urls.get('nrc_files', [])
                updated_residence = current_residence + file_urls.get('proof_residency_files', [])

                update_data = {
                    "nrc_files": updated_nrc,
                    "proof_residency_files": updated_residence
                }

                db_response = (
                    self.supabase
                    .table('borrower_files')
                    .update(update_data)
                    .eq('id', record_id)
                    .execute()
                )
            else:
                # Create new record
                new_record = {
                    "borrower_id": borrower_id,
                    "nrc_files": file_urls.get('nrc_files', []),
                    "proof_residency_files": file_urls.get('proof_residency_files', [])
                }

                db_response = (
                    self.supabase
                    .table('borrower_files')
                    .insert(new_record)
                    .execute()
                )

            if db_response.data:
                return {
                    "success": True,
                    "database_record": db_response.data[0],
                    "message": "File URLs saved to database successfully"
                }
            else:
                return {
                    "success": False,
                    "error": "Database operation failed",
                    "message": "Failed to save file URLs to database"
                }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Error saving to database: {str(e)}"
            }

    def complete_borrower_registration_with_files(self, form_data, request_files):
        """
        Complete method that handles the entire borrower registration process with files
        This works with your HTML form submission

        Args:
            form_data: request.form data (regular form fields)
            request_files: request.files data (uploaded files)

        Returns:
            dict: Complete registration result including borrower_id and file processing results
        """
        try:
            # Step 1: Create borrower record first (you'll need to implement this)
            # borrower_result = self.create_borrower(form_data)
            # borrower_id = borrower_result["borrower_id"]

            # For now, assuming you pass borrower_id or extract it somehow
            # You'll need to replace this with actual borrower creation logic
            borrower_id = form_data.get('borrower_id')  # Replace with actual logic

            if not borrower_id:
                return {
                    "success": False,
                    "error": "Borrower ID not available",
                    "message": "Cannot process files without borrower ID"
                }

            # Step 2: Prepare form data for file processing
            combined_data = {}

            # Add regular form fields
            for key, value in form_data.items():
                combined_data[key] = value

            # Add files from request.files
            for key, file in request_files.items():
                combined_data[key] = file

            # Step 3: Process all uploaded files
            file_processing_result = self.process_borrower_form_files(combined_data, borrower_id)

            if not file_processing_result["success"]:
                return {
                    "success": False,
                    "error": "File processing failed",
                    "message": file_processing_result.get("message", "Failed to process files"),
                    "file_processing_result": file_processing_result
                }

            # Step 4: Save file URLs to borrower_files table
            db_result = self.save_borrower_files_to_db(
                borrower_id=borrower_id,
                file_urls=file_processing_result["file_urls"]
            )

            return {
                "success": True,
                "borrower_id": borrower_id,
                "file_processing_result": file_processing_result,
                "database_result": db_result,
                "message": "Borrower registration with files completed successfully"
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Complete registration process failed: {str(e)}"
            }

    def complete_multiple_files_upload_process(self, files_data, save_to_db=True):
        """
        Complete upload process for multiple files: upload to bucket, get URLs, and optionally save records to database

        Args:
            files_data: List of dictionaries containing file info
            save_to_db: Whether to save file records to database (default: True)

        Returns:
            dict: Contains complete results for all uploads including database records if saved
        """
        if not files_data or not isinstance(files_data, list):
            return {
                "successful_uploads": [],
                "failed_uploads": [],
                "total_files": 0,
                "success_count": 0,
                "failure_count": 0,
                "message": "No files provided or invalid files_data format"
            }

        results = {
            "successful_uploads": [],
            "failed_uploads": [],
            "total_files": len(files_data),
            "database_records": []
        }

        for file_data in files_data:
            # Validate file_data structure
            required_keys = ['file_object', 'file_name', 'document_type']
            if not all(key in file_data for key in required_keys):
                results["failed_uploads"].append({
                    "file_name": file_data.get('file_name', 'Unknown'),
                    "error": "Missing required keys in file_data"
                })
                continue

            result = self.upload_borrower_file(
                file_object=file_data['file_object'],
                file_name=file_data['file_name'],
                document_type=file_data['document_type']
            )

            if result["success"]:
                results["successful_uploads"].append(result)
            else:
                results["failed_uploads"].append({
                    "file_name": file_data['file_name'],
                    "error": result["error"],
                    "message": result.get("message", "Upload failed")
                })

        results["success_count"] = len(results["successful_uploads"])
        results["failure_count"] = len(results["failed_uploads"])

        return results

    def handle_borrower_file_upload_from_form(self, request_files, form_data, borrower_id):
        """
        Simplified method to handle file uploads directly from Flask request

        Args:
            request_files: Flask request.files object
            form_data: Flask request.form object
            borrower_id: The borrower ID to associate files with

        Returns:
            dict: Complete upload and database save results
        """
        try:
            # Get total files from form
            total_files = int(form_data.get('total_files', 0))

            if total_files == 0:
                return {
                    "success": True,
                    "message": "No files to process",
                    "files_processed": 0
                }

            file_urls = {
                "nrc_files": [],
                "proof_residency_files": []
            }

            upload_results = {
                "successful_uploads": [],
                "failed_uploads": []
            }

            # Process each file
            for i in range(total_files):
                file_key = f'file_{i}'
                file_type_key = f'file_{i}_type'
                file_name_key = f'file_{i}_name'

                # Get the actual file object from request.files
                file_object = request_files.get(file_key)
                document_type = form_data.get(file_type_key)
                original_filename = form_data.get(file_name_key)

                if not file_object or not document_type:
                    upload_results["failed_uploads"].append({
                        "file_index": i,
                        "error": "Missing file or document type",
                        "file_name": original_filename or f"file_{i}"
                    })
                    continue

                # Upload to Supabase bucket
                upload_result = self.upload_borrower_file(
                    file_object=file_object.read(),  # Read the file content
                    file_name=original_filename or file_object.filename,
                    document_type=document_type
                )

                if upload_result["success"]:
                    upload_results["successful_uploads"].append(upload_result)

                    # Categorize the file URL based on document type
                    if document_type == "identity":
                        file_urls["nrc_files"].append(upload_result["file_url"])
                    elif document_type == "residence":
                        file_urls["proof_residency_files"].append(upload_result["file_url"])

                else:
                    upload_results["failed_uploads"].append({
                        "file_index": i,
                        "file_name": original_filename or file_object.filename,
                        "error": upload_result["error"],
                        "message": upload_result.get("message", "Upload failed")
                    })

            # Save file URLs to database if we have any successful uploads
            database_result = None
            if file_urls["nrc_files"] or file_urls["proof_residency_files"]:
                database_result = self.save_borrower_files_to_db(borrower_id, file_urls)

            # Calculate overall success
            overall_success = (
                    len(upload_results["failed_uploads"]) == 0 and
                    (database_result is None or database_result.get("success", False))
            )

            return {
                "success": overall_success,
                "message": f"Processed {len(upload_results['successful_uploads'])} files successfully" if overall_success else "Some operations failed",
                "files_processed": len(upload_results["successful_uploads"]),
                "files_failed": len(upload_results["failed_uploads"]),
                "upload_details": upload_results,
                "database_result": database_result,
                "file_urls": file_urls
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"File upload process failed: {str(e)}",
                "files_processed": 0
            }

    def create_borrower_with_files(self, form_data, request_files):
        """
        Complete method to create a borrower and handle file uploads

        Args:
            form_data: Flask request.form data
            request_files: Flask request.files data

        Returns:
            dict: Complete operation result
        """
        try:
            # Step 1: Create the borrower record first
            borrower_data = {
                "first_name": form_data.get('first_name'),
                "last_name": form_data.get('last_name'),
                "nrc_number": form_data.get('nrc_number'),
                "email": form_data.get('email'),
                "phone": form_data.get('phone'),
                "date_of_birth": form_data.get('date_of_birth'),
                "gender": form_data.get('gender'),
                "address": form_data.get('address'),
                "organisation_id": form_data.get('organisation_id') if form_data.get('organisation_id') else None,
                "occupation": form_data.get('occupation', 'Employee'),
                "employee_id": form_data.get('employee_id')
            }

            # Remove None values and empty strings
            borrower_data = {k: v for k, v in borrower_data.items() if v is not None and v != ''}

            # Insert borrower into database
            borrower_response = (
                self.supabase
                .table('borrowers')
                .insert(borrower_data)
                .execute()
            )

            if not borrower_response.data:
                return {
                    "success": False,
                    "error": "Failed to create borrower record",
                    "message": "Database insertion failed"
                }

            borrower_id = borrower_response.data[0]['id']
            print(f"Created borrower with ID: {borrower_id}")

            # Step 2: Handle next of kin if provided
            next_of_kin_id = None
            kin_first_name = form_data.get('kin_first_name')
            kin_last_name = form_data.get('kin_last_name')

            if kin_first_name and kin_last_name:
                kin_data = {
                    "first_name": kin_first_name,
                    "last_name": kin_last_name,
                    "email": form_data.get('kin_email'),
                    "phone": form_data.get('kin_phone'),
                }

                # Remove None values and empty strings
                kin_data = {k: v for k, v in kin_data.items() if v is not None and v != ''}

                try:
                    kin_response = (
                        self.supabase
                        .table('next_of_kins')
                        .insert(kin_data)
                        .execute()
                    )

                    if kin_response.data:
                        next_of_kin_id = kin_response.data[0]['id']
                        print(f"Created next of kin with ID: {next_of_kin_id}")

                        # Update borrower with next_of_kin_id
                        self.supabase.table('borrowers').update({
                            "next_of_kin_id": next_of_kin_id
                        }).eq('id', borrower_id).execute()

                except Exception as kin_error:
                    print(f"Warning: Failed to create next of kin: {kin_error}")

            # Step 3: Handle bank details
            bank_id = None
            bank_name = form_data.get('bank_name')

            if bank_name:
                bank_data = {
                    "borrower_id": borrower_id,
                    "bank_name": bank_name,
                    "branch_name": form_data.get('branch_name'),
                    "swift_code": form_data.get('swift_code'),
                    "account_number": form_data.get('account_number'),
                }

                # Remove None values and empty strings but keep borrower_id
                bank_data = {k: v for k, v in bank_data.items()
                             if (v is not None and v != '') or k == 'borrower_id'}

                try:
                    bank_response = (
                        self.supabase
                        .table('borrower_banks')
                        .insert(bank_data)
                        .execute()
                    )

                    if bank_response.data:
                        bank_id = bank_response.data[0]['id']
                        print(f"Created bank record with ID: {bank_id}")

                except Exception as bank_error:
                    print(f"Warning: Failed to create bank record: {bank_error}")

            # Step 4: Handle file uploads
            file_upload_result = self.handle_borrower_file_upload_from_form(
                request_files, form_data, borrower_id
            )

            print(f"File upload result: {file_upload_result}")

            # Return complete result
            return {
                "success": True,
                "borrower_id": borrower_id,
                "next_of_kin_id": next_of_kin_id,
                "bank_id": bank_id,
                "file_upload_result": file_upload_result,
                "message": "Borrower created successfully with files and bank details"
            }

        except Exception as e:
            print(f"Error in create_borrower_with_files: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to create borrower: {str(e)}"
            }

    def update_borrower_with_files(self, borrower_id, form_data, request_files):
        """
        Update an existing borrower and handle file uploads

        Args:
            borrower_id: The ID of the borrower to update
            form_data: Flask request.form data
            request_files: Flask request.files data

        Returns:
            dict: Complete update operation result
        """
        try:
            # Step 1: Update the main borrower record
            borrower_data = {
                "first_name": form_data.get('first_name'),
                "last_name": form_data.get('last_name'),
                "nrc_number": form_data.get('nrc_number'),
                "email": form_data.get('email'),
                "phone": form_data.get('phone'),
                "date_of_birth": form_data.get('date_of_birth'),
                "gender": form_data.get('gender'),
                "address": form_data.get('address'),
                "organisation_id": form_data.get('organisation_id') if form_data.get('organisation_id') else None,
                "occupation": form_data.get('occupation'),
                "employee_id": form_data.get('employee_id')
            }

            # Remove None values and empty strings
            borrower_data = {k: v for k, v in borrower_data.items() if v is not None and v != ''}

            # Update borrower in database
            borrower_response = (
                self.supabase
                .table('borrowers')
                .update(borrower_data)
                .eq('id', borrower_id)
                .execute()
            )

            if not borrower_response.data:
                return {
                    "success": False,
                    "error": "Failed to update borrower record",
                    "message": "Database update failed"
                }

            print(f"Updated borrower with ID: {borrower_id}")

            # Step 2: Handle next of kin updates
            kin_first_name = form_data.get('kin_first_name')
            kin_last_name = form_data.get('kin_last_name')

            if kin_first_name and kin_last_name:
                kin_data = {
                    "first_name": kin_first_name,
                    "last_name": kin_last_name,
                    "email": form_data.get('kin_email'),
                    "phone": form_data.get('kin_phone'),
                }

                # Remove None values and empty strings
                kin_data = {k: v for k, v in kin_data.items() if v is not None and v != ''}

                try:
                    # Check if borrower already has a next of kin
                    current_borrower = (
                        self.supabase
                        .table('borrowers')
                        .select('next_of_kin_id')
                        .eq('id', borrower_id)
                        .limit(1)
                        .execute()
                    )

                    if current_borrower.data and current_borrower.data[0].get('next_of_kin_id'):
                        # Update existing next of kin
                        next_of_kin_id = current_borrower.data[0]['next_of_kin_id']
                        kin_response = (
                            self.supabase
                            .table('next_of_kins')
                            .update(kin_data)
                            .eq('id', next_of_kin_id)
                            .execute()
                        )
                        print(f"Updated existing next of kin with ID: {next_of_kin_id}")
                    else:
                        # Create new next of kin
                        kin_response = (
                            self.supabase
                            .table('next_of_kins')
                            .insert(kin_data)
                            .execute()
                        )

                        if kin_response.data:
                            next_of_kin_id = kin_response.data[0]['id']
                            # Update borrower with new next_of_kin_id
                            self.supabase.table('borrowers').update({
                                "next_of_kin_id": next_of_kin_id
                            }).eq('id', borrower_id).execute()
                            print(f"Created new next of kin with ID: {next_of_kin_id}")

                except Exception as kin_error:
                    print(f"Warning: Failed to update next of kin: {kin_error}")

            # Step 3: Handle bank details updates
            bank_name = form_data.get('bank_name')

            if bank_name:
                bank_data = {
                    "bank_name": bank_name,
                    "branch_name": form_data.get('branch_name'),
                    "swift_code": form_data.get('swift_code'),
                    "account_number": form_data.get('account_number'),
                }

                # Remove None values and empty strings
                bank_data = {k: v for k, v in bank_data.items() if v is not None and v != ''}

                try:
                    # Check if borrower already has bank details
                    existing_bank = (
                        self.supabase
                        .table('borrower_banks')
                        .select('id')
                        .eq('borrower_id', borrower_id)
                        .limit(1)
                        .execute()
                    )

                    if existing_bank.data:
                        # Update existing bank record
                        bank_id = existing_bank.data[0]['id']
                        bank_response = (
                            self.supabase
                            .table('borrower_banks')
                            .update(bank_data)
                            .eq('id', bank_id)
                            .execute()
                        )
                        print(f"Updated existing bank record with ID: {bank_id}")
                    else:
                        # Create new bank record
                        bank_data['borrower_id'] = borrower_id
                        bank_response = (
                            self.supabase
                            .table('borrower_banks')
                            .insert(bank_data)
                            .execute()
                        )
                        if bank_response.data:
                            print(f"Created new bank record with ID: {bank_response.data[0]['id']}")

                except Exception as bank_error:
                    print(f"Warning: Failed to update bank details: {bank_error}")

            # Step 4: Handle file uploads (if any new files are provided)
            file_upload_result = self.handle_borrower_file_upload_from_form(
                request_files, form_data, borrower_id
            )

            print(f"File upload result: {file_upload_result}")

            # Return complete result
            return {
                "success": True,
                "borrower_id": borrower_id,
                "file_upload_result": file_upload_result,
                "message": "Borrower updated successfully with files and bank details"
            }

        except Exception as e:
            print(f"Error in update_borrower_with_files: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to update borrower: {str(e)}"
            }


    def get_borrower_edit_data(self, borrower_id):
        """
        Gets the comprehensive data needed to fill in the edit form for a borrower

        Args:
            borrower_id: The ID of the borrower to fetch data for

        Returns:
            dict: Contains borrower data with next_of_kin and bank_info, or error info
        """
        try:
            if not borrower_id:
                return {
                    "success": False,
                    "error": "Borrower ID is required",
                    "message": "Cannot fetch data without borrower ID"
                }

            # Step 1: Get the main borrower record
            borrower_response = (
                self.supabase
                .table('borrowers')
                .select('*')
                .eq('id', borrower_id)
                .limit(1)
                .execute()
            )

            if not borrower_response.data:
                return {
                    "success": False,
                    "error": "Borrower not found",
                    "message": f"No borrower found with ID: {borrower_id}"
                }

            borrower_data = borrower_response.data[0]

            # Step 2: Get next of kin information if next_of_kin_id exists
            next_of_kin_id = borrower_data.get('next_of_kin_id')
            if next_of_kin_id:
                try:
                    nok_response = (
                        self.supabase
                        .table('next_of_kins')
                        .select('first_name, last_name, email, phone')
                        .eq('id', next_of_kin_id)
                        .limit(1)
                        .execute()
                    )

                    if nok_response.data:
                        borrower_data['next_of_kin'] = nok_response.data[0]
                    else:
                        borrower_data['next_of_kin'] = None
                except Exception as e:
                    print(f"Error fetching next of kin for ID {next_of_kin_id}: {e}")
                    borrower_data['next_of_kin'] = None
            else:
                borrower_data['next_of_kin'] = None

            # Step 3: Get bank information
            try:
                bank_response = (
                    self.supabase
                    .table('borrower_banks')
                    .select('bank_name, branch_name, swift_code, account_number')
                    .eq('borrower_id', borrower_id)
                    .limit(1)
                    .execute()
                )

                if bank_response.data:
                    borrower_data['bank_info'] = bank_response.data[0]
                else:
                    borrower_data['bank_info'] = None
            except Exception as e:
                print(f"Error fetching bank info for borrower ID {borrower_id}: {e}")
                borrower_data['bank_info'] = None

            # Step 4: Get file information (optional - in case you want to show existing files)
            try:
                files_response = (
                    self.supabase
                    .table('borrower_files')
                    .select('nrc_files, proof_residency_files')
                    .eq('borrower_id', borrower_id)
                    .limit(1)
                    .execute()
                )

                if files_response.data:
                    borrower_data['files'] = files_response.data[0]
                else:
                    borrower_data['files'] = None
            except Exception as e:
                print(f"Error fetching files for borrower ID {borrower_id}: {e}")
                borrower_data['files'] = None

            return {
                "success": True,
                "borrower_data": borrower_data,
                "message": "Borrower edit data retrieved successfully"
            }

        except Exception as e:
            print(f'Exception in get_borrower_edit_data: {e}')
            return {
                "success": False,
                "error": str(e),
                "message": f"Error retrieving borrower edit data: {str(e)}"
            }

    def get_borrower_name(self, borrower_id):
        """get borrower name using their id"""
        try:
            response = (
                self.supabase
                .table('borrowers')
                .select('first_name','last_name', 'id')
                .eq('id', borrower_id)
                .execute()
            )

            return f'{response.data[0]['first_name']} {response.data[0]['last_name']}'

        except Exception as e:
            print(f'Exception: {e}')

# Remove the test execution from the class file
# test = Borrowers()
# print(test.exhaust_borrower_information())