import requests
import csv
import json
from datetime import datetime
import urllib.parse
import webbrowser
import secrets
import hashlib
import base64
from config import CLIENT_ID, CLIENT_SECRET, SUBSCRIPTION_KEY, STUDENT_ROLE_ID


class BlackbaudSISExporter:
    def __init__(self, client_id, client_secret, subscription_key, redirect_uri="http://localhost:8080"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.subscription_key = subscription_key
        self.redirect_uri = redirect_uri
        self.base_url = "https://api.sky.blackbaud.com/school"
        self.access_token = None

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
            return True
        else:
            print(f"Token exchange failed: {response.text}")
            return False

    def authenticate(self):
        """Handle OAuth 2.0 authorization code flow"""
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
        """Get all courses for a specific student across multiple school years"""
        if school_year_ids is None:
            school_year_ids = ["2025-2026", "2024-2025", "2023-2024", "2022-2023"]  # 2025-26, 2024-25, 2023-24, 2022-23

        all_courses = []

        for year_id in school_year_ids:
            endpoint = f"v1/academics/enrollments/{student_id}?school_year={year_id}"

            response = self.make_api_request(endpoint)

            if response and 'value' in response:
                for enrollment in response['value']:
                    # Only include courses where dropped is 0
                    if enrollment.get('dropped', 1) == 0:
                        course_title = enrollment.get('course_title')
                        if course_title and course_title not in all_courses:
                            all_courses.append(course_title)

        return all_courses

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

            courses = self.get_student_courses(senior['id'])

            # Join all course titles with | delimiter
            course_list = ' | '.join(courses) if courses else 'No courses found'

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

    exporter = BlackbaudSISExporter(CLIENT_ID, CLIENT_SECRET, SUBSCRIPTION_KEY)

    # Export seniors and their courses
    success = exporter.export_senior_courses_to_csv("class_of_2026_courses.csv", STUDENT_ROLE_ID)

    if success:
        print("Senior course export completed successfully!")
    else:
        print("Export failed. Check your credentials and API access.")
