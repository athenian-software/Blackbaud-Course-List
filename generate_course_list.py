import requests
import csv
import urllib.parse
import webbrowser
import secrets
import hashlib
import base64
import os.path
import json
import re
import argparse
import time
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import CLIENT_ID, CLIENT_SECRET, SUBSCRIPTION_KEY, STUDENT_ROLE_ID

# Google Sheets API scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Cache file for student data
STUDENT_DATA_CACHE = 'student_data_cache.json'

# Department configuration
DEPARTMENT_CONFIG = {
    'skip_courses': [
        'Athenian Wilderness Experience',
        'College Counseling',
        'Community Service',
        'AM Mtg',
        'Grade Dean',
        'US Advisory'
    ],
    'skip_course_prefixes': [
        'MT:'
    ],
    'skip_departments': [
        # Add department names to skip entirely
    ],
    'primary_departments': [
        # Primary departments in display order
        'Literature',
        'History/Social Science',
        'Math',
        'Science',
        'Computer Science and Engineering',
        'World Languages',
        'Fine Arts',
        'Other'  # Catch-all for non-primary departments
    ],
    'department_remapping': {
        # Map original department names to custom column names
        # e.g., 'Mathematics': 'Math', 'Science': 'STEM'
    },
    'course_overrides': {
        # Map specific course names to custom department columns
        # e.g., 'AP Computer Science': 'STEM'
    }
}


class BlackbaudSISExporter:
    def __init__(self, client_id, client_secret, subscription_key, redirect_uri="http://localhost:8080", token_file="blackbaud_token.json"):
        self.code_verifier = None
        self.client_id = client_id
        self.client_secret = client_secret
        self.subscription_key = subscription_key
        self.redirect_uri = redirect_uri
        self.base_url = "https://api.sky.blackbaud.com/school"
        self.access_token = None
        self.token_file = token_file

    def get_authorization_url(self):
        """Generate authorization URL for user to visit"""
        # Generate PKCE parameters
        code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8').rstrip('=')
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode('utf-8')).digest()
        ).decode('utf-8').rstrip('=')

        self.code_verifier = code_verifier

        auth_params = {
            'client_id': self.client_id,
            'response_type': 'code',
            'redirect_uri': self.redirect_uri,
            'code_challenge': code_challenge,
            'code_challenge_method': 'S256',
            'state': secrets.token_urlsafe(32)
        }

        auth_url = f"https://oauth2.sky.blackbaud.com/authorization?{urllib.parse.urlencode(auth_params)}"
        return auth_url

    def save_token(self, token_data):
        """Save token data to file"""
        # Add expiration timestamp
        token_data['expires_at'] = (datetime.now() + timedelta(seconds=token_data.get('expires_in', 3600))).isoformat()

        with open(self.token_file, 'w') as f:
            json.dump(token_data, f)

    def load_token(self):
        """Load token from file if it exists and is valid"""
        if not os.path.exists(self.token_file):
            return None

        try:
            with open(self.token_file, 'r') as f:
                token_data = json.load(f)

            # Check if token is expired
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            if datetime.now() >= expires_at:
                print("Saved token has expired")
                return None

            return token_data
        except Exception as e:
            print(f"Error loading token: {e}")
            return None

    def authenticate_with_code(self, authorization_code):
        """Exchange authorization code for access token"""
        token_url = "https://oauth2.sky.blackbaud.com/token"

        data = {
            'grant_type': 'authorization_code',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'code': authorization_code,
            'redirect_uri': self.redirect_uri,
            'code_verifier': self.code_verifier
        }

        response = requests.post(token_url, data=data)
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data['access_token']

            # Save token for future use
            self.save_token(token_data)

            return True
        else:
            print(f"Token exchange failed: {response.text}")
            return False

    def authenticate(self):
        """Handle OAuth 2.0 authorization code flow with token caching"""
        # Try to load existing token first
        token_data = self.load_token()
        if token_data:
            self.access_token = token_data['access_token']
            print("Using saved authentication token")
            return True

        # No valid token found, proceed with OAuth flow
        print("No valid saved token found, starting authentication flow...")

        # Step 1: Get authorization URL
        auth_url = self.get_authorization_url()

        print("Please visit this URL to authorize the application:")
        print(auth_url)
        print("\nOpening browser...")

        try:
            webbrowser.open(auth_url)
        except:
            print("Could not open browser automatically. Please copy the URL above.")

        # Step 2: Get authorization code from user
        print("\nAfter authorizing, you'll be redirected to a URL that starts with:")
        print(f"{self.redirect_uri}?code=...")
        print("\nPlease copy the 'code' parameter from that URL and paste it here:")

        auth_code = input("Authorization code: ").strip()

        # Step 3: Exchange code for token
        return self.authenticate_with_code(auth_code)

    def make_api_request(self, endpoint, params=None):
        """Make authenticated API request"""
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Bb-Api-Subscription-Key': self.subscription_key,
            'Content-Type': 'application/json'
        }

        url = f"{self.base_url}/{endpoint}"
        print(f"Making request to: {url}")
        if params:
            print(f"With parameters: {params}")

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            print(f"Authentication failed (401) for {url}")
            print(f"Response: {response.text}")
            print("This could mean:")
            print("1. Access token is invalid or expired")
            print("2. Missing required API permissions/scopes")
            print("3. Endpoint requires different authentication")
            return None
        else:
            print(f"API request failed: {response.status_code} - {response.text}")
            print(f"URL: {url}")
            return None

    def get_seniors(self, graduation_year=2026, role_id=None):
        """Get all students graduating in specified year (handles pagination)"""
        params = {
            'grad_year': 2025,
            'end_grad_year': graduation_year
        }

        if role_id:
            params['roles'] = role_id

        seniors = []
        endpoint = 'v1/users'

        while endpoint:
            response = self.make_api_request(endpoint, params)

            if not response:
                print("Could not retrieve students")
                break

            if 'value' in response:
                for student in response['value']:
                    seniors.append({
                        'id': student.get('id'),
                        'first_name': student.get('first_name'),
                        'last_name': student.get('last_name'),
                        'email': student.get('email'),
                        'grad_year': student.get('grad_year')
                    })

            # Check for next page
            next_link = response.get('next_link')
            if next_link:
                # Extract the endpoint from the full URL
                endpoint = next_link.replace(self.base_url + '/', '')
                params = None  # Next link already includes all params
            else:
                endpoint = None

        return seniors

    def get_student_courses(self, student_id, school_year_ids=None):
        """Get all courses for a specific student across multiple school years, organized by department"""
        if school_year_ids is None:
            school_year_ids = ["2025-2026", "2024-2025", "2023-2024", "2022-2023"]  # 2025-26, 2024-25, 2023-24, 2022-23

        courses_by_dept = {}
        seen_courses = set()

        for year_id in school_year_ids:
            endpoint = f"v1/academics/enrollments/{student_id}?school_year={year_id}"

            response = self.make_api_request(endpoint)

            if response and 'value' in response:
                for enrollment in response['value']:
                    # Only include courses where dropped is 0
                    if enrollment.get('dropped', 1) == 0:
                        course_title = enrollment.get('course_title')

                        # Skip duplicates
                        if course_title and course_title not in seen_courses:
                            seen_courses.add(course_title)

                            # Check if course should be skipped
                            if self._should_skip_course(course_title):
                                continue

                            # Check for special course prefixes that override department
                            special_dept = self._check_special_course_prefix(course_title)
                            if special_dept:
                                dept_name = special_dept
                            # Check block_name - if it doesn't contain A-G, put in "Other"
                            elif not any(letter in enrollment.get('block_name', '') for letter in 'ABCDEFG'):
                                dept_name = 'Other'
                            else:
                                # Get department name
                                departments = enrollment.get('departments', [])

                                if not departments:
                                    dept_name = 'Other'
                                elif departments[0].get('name') == 'Humanities':
                                    # If primary department is Humanities, use second department
                                    if len(departments) > 1:
                                        dept_name = departments[1].get('name', 'Other')
                                    elif (course_title == 'Social Psychology (H)'
                                            or course_title == "Politics of Elections (H)'"):
                                        dept_name = 'History/Social Science'
                                    else:
                                        print(f"ERROR: Course '{course_title}' has 'Humanities' as primary department but no second department")
                                        dept_name = 'Other'
                                else:
                                    dept_name = departments[0].get('name')

                                # Apply configuration overrides (only if not already set by special prefix)
                                dept_name = self._apply_department_config(course_title, dept_name)

                            # Skip if department is in skip list
                            if dept_name is None:
                                continue

                            # Log warning if course with (H) ends up in Other
                            if '(H)' in course_title and dept_name == 'Other':
                                print(f"WARNING: Course '{course_title}' contains '(H)' but was categorized as 'Other'")

                            # Add course to department
                            if dept_name not in courses_by_dept:
                                courses_by_dept[dept_name] = []
                            courses_by_dept[dept_name].append(course_title)

        return courses_by_dept

    def _should_skip_course(self, course_title):
        """Check if a course should be skipped based on configuration"""
        # Check for exact match
        if course_title in DEPARTMENT_CONFIG['skip_courses']:
            return True

        # Check for prefix match
        for prefix in DEPARTMENT_CONFIG['skip_course_prefixes']:
            if course_title.startswith(prefix):
                return True

        return False

    def _check_special_course_prefix(self, course_title):
        """Check for special course prefixes that override department assignment"""
        # Teacher's Assistant → Other
        if course_title.startswith("Teacher's Assistant"):
            return 'Other'

        # HistX (where X is a digit) → History/Social Science
        if re.match(r'^Hist\d', course_title):
            return 'History/Social Science'

        # EngX (where X is a digit) → Literature
        if re.match(r'^Eng\d', course_title):
            return 'Literature'

        return None

    def _apply_department_config(self, course_title, original_dept):
        """Apply department configuration overrides"""
        # Check if department should be skipped (exact match)
        if original_dept in DEPARTMENT_CONFIG['skip_departments']:
            return None

        # Check for course-specific overrides first (highest priority)
        if course_title in DEPARTMENT_CONFIG['course_overrides']:
            final_dept = DEPARTMENT_CONFIG['course_overrides'][course_title]
        # Check for department remapping
        elif original_dept in DEPARTMENT_CONFIG['department_remapping']:
            final_dept = DEPARTMENT_CONFIG['department_remapping'][original_dept]
        else:
            final_dept = original_dept

        # If the final department is not in the primary list, map to "Other"
        if final_dept not in DEPARTMENT_CONFIG['primary_departments']:
            return 'Other'

        return final_dept

    def test_authentication(self):
        """Test authentication by making a simple API request"""
        print("Testing authentication...")

        # Test with a known working endpoint
        result = self.make_api_request("v1/users", {'roles': '11821', 'grad_year': 2025, 'end_grad_year': 2026})

        if result is not None:
            print("✓ Authentication successful")
            return True
        else:
            print("✗ Authentication failed")
            return False

    def save_student_data_cache(self, students_data, cache_file=STUDENT_DATA_CACHE):
        """Save student data to cache file"""
        try:
            with open(cache_file, 'w') as f:
                json.dump(students_data, f, indent=2)
            print(f"Cached student data to {cache_file}")
        except Exception as e:
            print(f"Warning: Could not save cache: {e}")

    def load_student_data_cache(self, cache_file=STUDENT_DATA_CACHE):
        """Load student data from cache file if it exists"""
        if not os.path.exists(cache_file):
            return None

        try:
            with open(cache_file, 'r') as f:
                students_data = json.load(f)
            print(f"Loaded {len(students_data)} students from cache ({cache_file})")
            return students_data
        except Exception as e:
            print(f"Warning: Could not load cache: {e}")
            return None

    def get_google_sheets_service(self):
        """Authenticate and return Google Sheets service"""
        creds = None
        # The file token.json stores the user's access and refresh tokens
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        return build('sheets', 'v4', credentials=creds)

    def export_senior_courses_to_google_sheet(self, sheet_title="Class of 2026 Courses", role_id=None, limit_students=None, reload=False):
        """Export senior courses to Google Sheet with tabs"""

        # Try to load from cache if not reloading
        students_data = None
        if not reload:
            students_data = self.load_student_data_cache()

        # If no cached data or reload requested, fetch from API
        if students_data is None:
            if not self.authenticate():
                return False

            # Test authentication first
            if not self.test_authentication():
                print("Authentication test failed. Check your API permissions.")
                return False

            print("Getting list of seniors...")
            seniors = self.get_seniors(2026, role_id)
            print(f"Found {len(seniors)} seniors")

            # Limit students for testing
            if limit_students:
                seniors = seniors[:limit_students]
                print(f"Limiting to first {limit_students} students for testing")

            # Gather all student data
            students_data = []
            for i, senior in enumerate(seniors, 1):
                print(f"Processing student {i}/{len(seniors)}: {senior['first_name']} {senior['last_name']}")

                courses = self.get_student_courses(senior['id'])

                students_data.append({
                    'id': senior['id'],
                    'first_name': senior['first_name'],
                    'last_name': senior['last_name'],
                    'email': senior['email'],
                    'grad_year': senior['grad_year'],
                    'courses': courses
                })

            # Save to cache
            self.save_student_data_cache(students_data)
        else:
            # Apply limit to cached data if specified
            if limit_students and len(students_data) > limit_students:
                students_data = students_data[:limit_students]
                print(f"Limiting to first {limit_students} students from cache")

        # Authenticate with Google Sheets API
        print("\nAuthenticating with Google Sheets API...")
        sheets_service = self.get_google_sheets_service()

        # Create spreadsheet with tabs
        print("Creating Google Sheet with tabs...")
        spreadsheet = self._create_sheet_with_tabs(sheets_service, sheet_title, students_data)

        if spreadsheet:
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            print(f"\n✓ Spreadsheet created successfully!")
            print(f"Spreadsheet ID: {spreadsheet_id}")
            print(f"View at: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")
            return spreadsheet_id
        else:
            print("Failed to create spreadsheet")
            return None

    def _create_sheet_with_tabs(self, sheets_service, title, students_data):
        """Create a Google Sheet with a tab for each student"""
        try:
            # Create spreadsheet with first student's name as the first sheet
            first_student_name = f"{students_data[0]['first_name']} {students_data[0]['last_name']}"

            spreadsheet = {
                'properties': {
                    'title': title
                },
                'sheets': [
                    {
                        'properties': {
                            'title': first_student_name
                        }
                    }
                ]
            }

            created_spreadsheet = sheets_service.spreadsheets().create(
                body=spreadsheet
            ).execute()

            spreadsheet_id = created_spreadsheet.get('spreadsheetId')
            print(f"Spreadsheet created: {spreadsheet_id}")

            # Add additional sheets for remaining students
            if len(students_data) > 1:
                requests = []
                for student in students_data[1:]:
                    student_name = f"{student['first_name']} {student['last_name']}"
                    requests.append({
                        'addSheet': {
                            'properties': {
                                'title': student_name
                            }
                        }
                    })

                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                ).execute()
                print(f"Added {len(requests)} additional sheets")

            # Populate each sheet with student data
            print("Populating sheets with student data...")
            for i, student in enumerate(students_data):
                self._populate_student_sheet(sheets_service, spreadsheet_id, student)
                # Rate limiting: Google Sheets API allows 60 write requests per minute
                # Each student takes ~2 requests, so delay 2 seconds between students
                if i < len(students_data) - 1:  # Don't delay after last student
                    time.sleep(2)

            # Create summary sheet after all student sheets are populated
            self._create_summary_sheet(sheets_service, spreadsheet_id, students_data)

            return created_spreadsheet
        except HttpError as err:
            print(f"An error occurred: {err}")
            return None

    def _populate_student_sheet(self, sheets_service, spreadsheet_id, student):
        """Populate a sheet with student name and course data organized by department"""
        student_name = f"{student['first_name']} {student['last_name']}"
        print(f"  Populating sheet for {student_name}...")

        courses_by_dept = student['courses']

        # Prepare data for the sheet
        # Split departments into two groups to avoid having a very wide sheet
        # Top section: First 4 departments
        # Bottom section: Last 4 departments
        # Allow for max 10 courses per department

        # Order departments: primary departments only (in specified order)
        # All courses should now be in primary departments (with non-primary mapped to "Other")
        departments = []

        # Add primary departments that the student has courses in
        for dept in DEPARTMENT_CONFIG['primary_departments']:
            if dept in courses_by_dept:
                departments.append(dept)

        # Split into top and bottom sections
        top_departments = departments[:4]
        bottom_departments = departments[4:]

        # Calculate total scores for each department (we'll need these for the header)
        dept_totals = {}
        for dept in departments:
            courses = courses_by_dept.get(dept, [])
            total = 0
            for i, course_name in enumerate(courses):
                score = self._calculate_course_score(dept, course_name, i, courses)
                total += score
            dept_totals[dept] = total

        # Calculate overall curriculum rating
        curriculum_rating = sum(dept_totals.values())

        # Row 1: Student name and curriculum rating
        data = [[student_name, '', 'Curriculum Rating:', curriculum_rating]]

        # Row 2: Empty
        data.append([])

        # Top Section (first 4 departments)
        # Row 3: Department headers for top section
        header_row = []
        for dept in top_departments:
            header_row.append(dept_totals[dept] if dept_totals[dept] > 0 else '')  # Sum of scores for this department
            header_row.append(dept)  # Department name for course column
        data.append(header_row)

        # Rows 4-13: Up to 10 courses for top departments
        for i in range(10):
            row = []
            for dept in top_departments:
                courses = courses_by_dept.get(dept, [])
                if i < len(courses):
                    course_name = courses[i]
                    score = self._calculate_course_score(dept, course_name, i, courses)
                    row.append(score if score > 0 else '')  # Score column (empty if 0)
                    row.append(course_name)  # Course name
                else:
                    row.append('')  # Empty score cell
                    row.append('')  # Empty course cell
            data.append(row)

        # Row 14: Empty separator
        data.append([])

        # Bottom Section (last 4 departments)
        # Calculate max courses in bottom section and end row
        max_bottom_courses = 0
        bottom_end_row = 15  # Default if no bottom departments

        # Row 15: Department headers for bottom section
        if bottom_departments:
            header_row = []
            for dept in bottom_departments:
                header_row.append(dept_totals[dept] if dept_totals[dept] > 0 else '')  # Sum of scores for this department
                header_row.append(dept)  # Department name for course column
            data.append(header_row)

            # Find the max number of courses in bottom departments (no limit)
            max_bottom_courses = max(len(courses_by_dept.get(dept, [])) for dept in bottom_departments)
            bottom_end_row = 15 + max_bottom_courses

            # Rows 16+: All courses for bottom departments (dynamic based on actual count)
            for i in range(max_bottom_courses):
                row = []
                for dept in bottom_departments:
                    courses = courses_by_dept.get(dept, [])
                    if i < len(courses):
                        course_name = courses[i]
                        score = self._calculate_course_score(dept, course_name, i, courses)
                        row.append(score if score > 0 else '')  # Score column (empty if 0)
                        row.append(course_name)  # Course name
                    else:
                        row.append('')  # Empty score cell
                        row.append('')  # Empty course cell
                data.append(row)

        # Update the sheet
        try:
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{student_name}'!A1",
                valueInputOption='RAW',
                body={'values': data}
            ).execute()

            # Format the header rows and set column widths
            sheet_id = self._get_sheet_id(sheets_service, spreadsheet_id, student_name)
            requests = [
                # Student name row - bold and larger font
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True,
                                    'fontSize': 14
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.textFormat'
                    }
                },
                # Curriculum Rating label - bold
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 2,
                            'endColumnIndex': 3
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True
                                },
                                'horizontalAlignment': 'RIGHT'
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
                    }
                },
                # Curriculum Rating value - bold and larger
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 3,
                            'endColumnIndex': 4
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True,
                                    'fontSize': 14
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.textFormat'
                    }
                },
                # Top section department headers row (row 3) - bold
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 3
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True
                                },
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
                    }
                }
            ]

            # Bottom section department headers row (row 15) - bold and centered
            if bottom_departments:
                # Calculate the end row for bottom section (header at 14, then max_bottom_courses rows)
                bottom_end_row = 15 + max_bottom_courses

                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 14,
                            'endRowIndex': 15
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True
                                },
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
                    }
                })

            # Set column widths
            # Top section departments
            for i in range(len(top_departments)):
                score_col_index = i * 2
                course_col_index = i * 2 + 1

                # Score columns - narrow (30 pixels for ~3 chars)
                requests.append({
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': score_col_index,
                            'endIndex': score_col_index + 1
                        },
                        'properties': {
                            'pixelSize': 30
                        },
                        'fields': 'pixelSize'
                    }
                })

                # Course columns - wider (250 pixels to avoid truncation)
                requests.append({
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': course_col_index,
                            'endIndex': course_col_index + 1
                        },
                        'properties': {
                            'pixelSize': 250
                        },
                        'fields': 'pixelSize'
                    }
                })

            # Define colors for departments (light pastels)
            dept_colors = [
                {'red': 0.95, 'green': 0.9, 'blue': 0.9},    # Light pink
                {'red': 0.9, 'green': 0.95, 'blue': 0.9},    # Light green
                {'red': 0.9, 'green': 0.9, 'blue': 0.95},    # Light blue
                {'red': 0.95, 'green': 0.95, 'blue': 0.9},   # Light yellow
                {'red': 0.95, 'green': 0.9, 'blue': 0.95},   # Light purple
                {'red': 0.9, 'green': 0.95, 'blue': 0.95},   # Light cyan
                {'red': 0.95, 'green': 0.92, 'blue': 0.9},   # Light orange
                {'red': 0.93, 'green': 0.93, 'blue': 0.93}   # Light gray
            ]

            # Add background colors and borders for top section departments
            for i, dept in enumerate(top_departments):
                col_start = i * 2  # Each dept has 2 columns (score + course)
                col_end = col_start + 2

                # Background color for entire department (rows 3-13)
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 13,
                            'startColumnIndex': col_start,
                            'endColumnIndex': col_end
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': dept_colors[i % len(dept_colors)]
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })

                # Center-align score column (rows 4-13)
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 3,
                            'endRowIndex': 13,
                            'startColumnIndex': col_start,
                            'endColumnIndex': col_start + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat.horizontalAlignment'
                    }
                })

                # Border around department
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 13,
                            'startColumnIndex': col_start,
                            'endColumnIndex': col_end
                        },
                        'top': {'style': 'SOLID', 'width': 2},
                        'bottom': {'style': 'SOLID', 'width': 2},
                        'left': {'style': 'SOLID', 'width': 2},
                        'right': {'style': 'SOLID', 'width': 2}
                    }
                })

            # Add background colors and borders for bottom section departments
            if bottom_departments:
                for i, dept in enumerate(bottom_departments):
                    col_start = i * 2
                    col_end = col_start + 2

                    # Background color for entire department (rows 15 to bottom_end_row)
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 14,
                                'endRowIndex': bottom_end_row,
                                'startColumnIndex': col_start,
                                'endColumnIndex': col_end
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'backgroundColor': dept_colors[(i + 4) % len(dept_colors)]
                                }
                            },
                            'fields': 'userEnteredFormat.backgroundColor'
                        }
                    })

                    # Center-align score column (rows 16 to bottom_end_row)
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 15,
                                'endRowIndex': bottom_end_row,
                                'startColumnIndex': col_start,
                                'endColumnIndex': col_start + 1
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'horizontalAlignment': 'CENTER'
                                }
                            },
                            'fields': 'userEnteredFormat.horizontalAlignment'
                        }
                    })

                    # Border around department
                    requests.append({
                        'updateBorders': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 14,
                                'endRowIndex': bottom_end_row,
                                'startColumnIndex': col_start,
                                'endColumnIndex': col_end
                            },
                            'top': {'style': 'SOLID', 'width': 2},
                            'bottom': {'style': 'SOLID', 'width': 2},
                            'left': {'style': 'SOLID', 'width': 2},
                            'right': {'style': 'SOLID', 'width': 2}
                        }
                    })

            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()

        except HttpError as err:
            print(f"Error populating sheet: {err}")

    def _calculate_course_score(self, department, course_name, course_index, all_courses):
        """Calculate the difficulty score for a course based on department-specific rules"""

        if department == 'Literature':
            # Exclude US History and World History from scoring
            excluded_courses = ['US Literature', 'World Literature']
            if course_name in excluded_courses:
                return 0

            # Count how many non-excluded courses come before this one
            non_excluded_count = 0
            for i in range(course_index + 1):
                if all_courses[i] not in excluded_courses:
                    non_excluded_count += 1

            # First 4 non-excluded courses are worth 0, additional courses worth 0.5
            if non_excluded_count <= 4:
                return 0
            else:
                return 0.5

        elif department == 'History/Social Science':
            # Exclude US History and World History from scoring
            excluded_courses = ['US History', 'World History']
            if course_name in excluded_courses:
                return 0

            # Count how many non-excluded courses come before this one
            non_excluded_count = 0
            for i in range(course_index + 1):
                if all_courses[i] not in excluded_courses:
                    non_excluded_count += 1

            # First 2 non-excluded courses are worth 0, additional courses worth 0.5
            if non_excluded_count <= 2:
                return 0
            else:
                return 0.5

        elif department == 'Math':
            # Specific courses worth 1.0
            high_value_courses = [
                'Calculus I (H)',
                'AP Calculus AB',
                'Calculus II (H)',
                'Statistics (H)',
                'Multivariable Calculus (H)'
            ]

            # Specific courses worth 0.5
            medium_value_courses = [
                'Algebra II (H)',
                'Pre-Calculus (H)',
                'Calculus'
            ]

            if course_name in high_value_courses:
                return 1.0
            elif course_name in medium_value_courses:
                return 0.5
            else:
                return 0

        elif department == 'Science':
            # Courses with (H) are worth 1.0
            if '(H)' in course_name:
                return 1.0
            # Anatomy and Physiology (without H) is worth 0.5
            elif course_name == 'Anatomy & Physiology':
                return 0.5
            else:
                return 0

        elif department == 'Computer Science and Engineering':
            # Specific courses worth 1.0
            high_value_courses = [
                'Computer Science II (H)',
                'Advanced Computer Science (H)'
            ]

            # Specific courses worth 0.5
            medium_value_courses = [
                'Advanced Topics in Computer Science (H)',
                'Data Structures and Algorithms (H)',
                'Software Engineering (H)'
            ]

            if course_name in high_value_courses:
                return 1.0
            elif course_name in medium_value_courses:
                return 0.5
            else:
                return 0

        elif department == 'World Languages':
            # Courses with IV (H) or V (H) are worth 1.0
            if 'IV (H)' in course_name or 'V (H)' in course_name:
                return 1.0
            # Courses with IV (no H) or III (H) are worth 0.5
            # Check for "IV" but not "IV (H)" - look for "IV" followed by space or end of string
            elif ('III (H)' in course_name or
                  (' IV' in course_name and 'IV (H)' not in course_name)):
                return 0.5
            else:
                return 0

        # Default: no score for other departments (yet)
        return 0

    def _get_sheet_id(self, sheets_service, spreadsheet_id, sheet_name):
        """Get the sheet ID for a given sheet name"""
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()

        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']
        return None

    def _get_highest_calculus_math(self, courses_by_dept):
        """Get the highest calculus-track math course for a student"""
        math_courses = courses_by_dept.get('Math', [])

        # Hierarchy from highest to lowest
        calculus_hierarchy = [
            'Multivariable Calculus (H)',
            'Calculus II (H)',
            'Calculus I (H)',
            'Calculus',
            'Pre-Calculus (H)',
            'Pre-Calculus',
            'Algebra II (H)',
            'Algebra II'
        ]

        for course in calculus_hierarchy:
            if course in math_courses:
                return course

        return 'None'

    def _get_highest_world_language(self, courses_by_dept):
        """Get the highest world language level for a student"""
        wl_courses = courses_by_dept.get('World Languages', [])

        # Hierarchy from highest to lowest
        # Pattern: "<Language> <Level> [(H)]"
        level_hierarchy = [
            'V (H)',
            'IV (H)',
            ' IV',  # Space before to avoid matching "IV (H)"
            'III',
            ' II',  # Space before to avoid matching other levels
            ' I'    # Space before to avoid matching other levels
        ]

        for level in level_hierarchy:
            for course in wl_courses:
                if level in course:
                    # Return just the level part (e.g., "V (H)", "IV", etc.)
                    if level == 'V (H)':
                        return 'V (H)'
                    elif level == 'IV (H)':
                        return 'IV (H)'
                    elif level == ' IV':
                        return 'IV'
                    elif level == 'III':
                        # Check if it's III (H) or just III
                        if 'III (H)' in course:
                            return 'III (H)'
                        else:
                            return 'III'
                    elif level == ' II':
                        if 'II (H)' in course:
                            return 'II (H)'
                        else:
                            return 'II'
                    elif level == ' I':
                        return 'I'

        return 'None'

    def _get_additional_math(self, courses_by_dept):
        """Get additional math courses (Statistics, Financial Math)"""
        math_courses = courses_by_dept.get('Math', [])

        has_statistics = any('Statistics' in course for course in math_courses)
        has_financial = any('Financial Math' in course for course in math_courses)

        if has_statistics and has_financial:
            return 'Statistics, Financial Math'
        elif has_statistics:
            return 'Statistics'
        elif has_financial:
            return 'Financial Math'
        else:
            return 'None'

    def _create_summary_sheet(self, sheets_service, spreadsheet_id, students_data):
        """Create a summary sheet with student information and hyperlinks to individual tabs"""
        print("Creating summary sheet...")

        # Calculate curriculum ratings for each student (needed for sorting)
        student_summaries = []
        for student in students_data:
            courses_by_dept = student['courses']

            # Calculate curriculum rating
            dept_totals = {}
            departments = []
            for dept in DEPARTMENT_CONFIG['primary_departments']:
                if dept in courses_by_dept:
                    departments.append(dept)

            for dept in departments:
                courses = courses_by_dept.get(dept, [])
                total = 0
                for i, course_name in enumerate(courses):
                    score = self._calculate_course_score(dept, course_name, i, courses)
                    total += score
                dept_totals[dept] = total

            curriculum_rating = sum(dept_totals.values())

            # Get summary data
            student_summaries.append({
                'first_name': student['first_name'],
                'last_name': student['last_name'],
                'curriculum_rating': curriculum_rating,
                'highest_math': self._get_highest_calculus_math(courses_by_dept),
                'highest_language': self._get_highest_world_language(courses_by_dept),
                'additional_math': self._get_additional_math(courses_by_dept)
            })

        # Sort by last name
        student_summaries.sort(key=lambda x: x['last_name'])

        # Prepare data for the sheet
        # Header row
        data = [['Student Name', 'Curriculum Rating', 'Highest Math (Calculus Track)', 'Highest World Language', 'Additional Math']]

        # Student rows with formulas for hyperlinks
        for summary in student_summaries:
            student_name = f"{summary['first_name']} {summary['last_name']}"
            # We'll use a formula to create a hyperlink
            # Format: =HYPERLINK("#gid=SHEET_ID", "Student Name")
            # We'll need to get the sheet ID for each student's tab
            data.append([
                student_name,  # We'll replace this with a formula later
                summary['curriculum_rating'],
                summary['highest_math'],
                summary['highest_language'],
                summary['additional_math']
            ])

        # Create the "Summary" sheet first
        try:
            # Add the summary sheet
            add_sheet_request = {
                'addSheet': {
                    'properties': {
                        'title': 'Summary',
                        'index': 0  # Place at the beginning
                    }
                }
            }

            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': [add_sheet_request]}
            ).execute()

            print("Summary sheet created")

            # Populate with data
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="Summary!A1",
                valueInputOption='RAW',
                body={'values': data}
            ).execute()

            # Now update the student names to be hyperlinks
            # Get all sheet information to map student names to sheet IDs
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()

            sheet_map = {}
            for sheet in spreadsheet.get('sheets', []):
                sheet_title = sheet['properties']['title']
                sheet_id = sheet['properties']['sheetId']
                sheet_map[sheet_title] = sheet_id

            # Create formulas for hyperlinks
            summary_sheet_id = sheet_map.get('Summary')
            requests = []

            # Format header row - bold and centered
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': summary_sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True
                            },
                            'horizontalAlignment': 'CENTER'
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
                }
            })

            # Set column widths
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': summary_sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 1
                    },
                    'properties': {
                        'pixelSize': 200
                    },
                    'fields': 'pixelSize'
                }
            })

            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': summary_sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 1,
                        'endIndex': 2
                    },
                    'properties': {
                        'pixelSize': 150
                    },
                    'fields': 'pixelSize'
                }
            })

            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': summary_sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 2,
                        'endIndex': 5
                    },
                    'properties': {
                        'pixelSize': 220
                    },
                    'fields': 'pixelSize'
                }
            })

            # Center align all columns except the first
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': summary_sheet_id,
                        'startRowIndex': 1,
                        'startColumnIndex': 1,
                        'endColumnIndex': 5
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'CENTER'
                        }
                    },
                    'fields': 'userEnteredFormat.horizontalAlignment'
                }
            })

            # Add hyperlinks to student names
            for i, summary in enumerate(student_summaries):
                student_name = f"{summary['first_name']} {summary['last_name']}"
                target_sheet_id = sheet_map.get(student_name)

                if target_sheet_id is not None:
                    # Create hyperlink formula
                    formula = f'=HYPERLINK("#gid={target_sheet_id}", "{student_name}")'

                    requests.append({
                        'updateCells': {
                            'range': {
                                'sheetId': summary_sheet_id,
                                'startRowIndex': i + 1,
                                'endRowIndex': i + 2,
                                'startColumnIndex': 0,
                                'endColumnIndex': 1
                            },
                            'rows': [{
                                'values': [{
                                    'userEnteredValue': {
                                        'formulaValue': formula
                                    }
                                }]
                            }],
                            'fields': 'userEnteredValue'
                        }
                    })

            # Execute all formatting requests
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()

            print("Summary sheet populated and formatted")

        except HttpError as err:
            print(f"Error creating summary sheet: {err}")

    def export_senior_courses_to_csv(self, filename="senior_courses.csv", role_id=None):
        """Main export function"""
        if not self.authenticate():
            return False

        # Test authentication first
        if not self.test_authentication():
            print("Authentication test failed. Check your API permissions.")
            return False

        print("Getting list of seniors...")
        seniors = self.get_seniors(2026, role_id)
        print(f"Found {len(seniors)} seniors")

        # Prepare CSV data
        csv_data = []

        for i, senior in enumerate(seniors, 1):
            print(f"Processing student {i}/{len(seniors)}: {senior['first_name']} {senior['last_name']}")

            courses_by_dept = self.get_student_courses(senior['id'])

            # Flatten courses from all departments for CSV
            all_courses = []
            for dept_courses in courses_by_dept.values():
                all_courses.extend(dept_courses)

            # Join all course titles with | delimiter
            course_list = ' | '.join(all_courses) if all_courses else 'No courses found'

            csv_data.append({
                'Student_ID': senior['id'],
                'First_Name': senior['first_name'],
                'Last_Name': senior['last_name'],
                'Email': senior['email'],
                'Graduation_Year': senior['grad_year'],
                'Courses': course_list
            })

        # Write to CSV
        if csv_data:
            fieldnames = ['Student_ID', 'First_Name', 'Last_Name', 'Email',
                          'Graduation_Year', 'Courses']

            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_data)

            print(f"Export complete! {len(csv_data)} records written to {filename}")
            return True
        else:
            print("No data to export")
            return False


# Usage example
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Export senior course data to Google Sheets')
    parser.add_argument('--reload', action='store_true',
                        help='Reload data from API instead of using cache')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit to first N students for testing')
    args = parser.parse_args()

    exporter = BlackbaudSISExporter(CLIENT_ID, CLIENT_SECRET, SUBSCRIPTION_KEY)

    # Export seniors and their courses to Google Sheet
    spreadsheet_id = exporter.export_senior_courses_to_google_sheet(
        "Class of 2026 Courses",
        STUDENT_ROLE_ID,
        limit_students=args.limit,
        reload=args.reload
    )

    if spreadsheet_id:
        print("\nSenior course export to Google Sheet completed successfully!")
    else:
        print("\nExport failed. Check your credentials and API access.")
