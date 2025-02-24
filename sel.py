import argparse
import os
import requests
import sys
import time
from dotenv import load_dotenv

load_dotenv()

class SelogerAPI:
    def __init__(self, concurrency=1, annonce_details=False, tasks_file=None, max_pages=2):
        self.api_key = os.environ.get("LOBSTR_API_KEY")
        if not self.api_key:
            sys.exit("LOBSTR_API_KEY environment variable not set!")
        self.headers = {
            'Authorization': f'Token {self.api_key}',
            'Content-Type': 'application/json'
        }
        self.concurrency = concurrency
        self.annonce_details = annonce_details
        self.tasks_file = tasks_file
        self.max_pages = max_pages
        self.squid_id = None
        self.run_id = None

    @staticmethod
    def get_mime_type(file_path):
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            return 'text/csv'
        elif ext == '.tsv':
            return 'text/tab-separated-values'
        else:
            sys.exit("Invalid file extension. Valid values are: csv or tsv.")

    def create_squid(self):
        url = "https://api.lobstr.io/v1/squids/create"
        payload = {"crawler": "78f5839ee4b97c30e67eec391b907dd0"}
        print("Creating squid...")
        resp = requests.post(url, headers=self.headers, json=payload)
        if not resp.ok:
            sys.exit(f"Error creating squid: {resp.text}")
        self.squid_id = resp.json().get("id")
        if not self.squid_id:
            sys.exit("Squid ID not found!")
        print("Squid created with ID:", self.squid_id)

    def update_squid(self):
        url = f"https://api.lobstr.io/v1/squids/{self.squid_id}"
        payload = {
            "concurrency": self.concurrency,
            "export_unique_results": True,
            "no_line_breaks": True,
            "to_complete": False,
            "params": {
                "max_pages": self.max_pages,
                "fill_results_details": {"annonce_details": self.annonce_details}
            },
            "accounts": None,
            "run_notify": "on_success"
        }
        print("Updating squid...")
        resp = requests.post(url, headers=self.headers, json=payload)
        if not resp.ok:
            sys.exit(f"Error updating squid: {resp.text}")
        print("Squid updated.")

    def upload_tasks_file(self):
        url = f"https://api.lobstr.io/v1/squids/{self.squid_id}/tasks/upload"
        mime_type = self.get_mime_type(self.tasks_file)
        try:
            with open(self.tasks_file, 'rb') as f:
                files = [
                    ('file', (os.path.basename(self.tasks_file), f, mime_type))
                ]
                resp = requests.post(url, headers={'Authorization': f'Token {self.api_key}'}, files=files)
        except Exception as e:
            sys.exit(f"Error opening file: {e}")

        if not resp.ok:
            sys.exit(f"Error uploading tasks file: {resp.text}")
        task_upload_id = resp.json().get("task_id")
        if not task_upload_id:
            sys.exit("Task upload ID not found in response!")
        print("Tasks file uploaded. Upload Task ID:", task_upload_id)
        return task_upload_id

    def poll_task_upload_status(self, task_upload_id):
        url = f"https://api.lobstr.io/v1/tasks/upload/{task_upload_id}"
        print("Polling for tasks file upload status:")
        max_wait = 60
        interval = 5
        elapsed = 0
        while elapsed < max_wait:
            resp = requests.get(url, headers={'Authorization': f'Token {self.api_key}'})
            if not resp.ok:
                sys.exit(f"Error checking upload status: {resp.text}")
            status_info = resp.json()
            state = status_info.get("state", "")
            print(f"Upload state: {state}")
            if state.upper() == "SUCCESS":
                print("Tasks file upload completed successfully.")
                return
            time.sleep(interval)
            elapsed += interval
        sys.exit("Tasks file upload did not complete within expected time.")

    def delete_squid(self):
        url = f"https://api.lobstr.io/v1/squids/{self.squid_id}"
        print("No tasks file provided. Deleting squid...")
        resp = requests.delete(url, headers=self.headers)
        if not resp.ok:
            sys.exit(f"Error deleting squid: {resp.text}")
        print("Squid deleted:", resp.json())

    def start_run(self):
        url = "https://api.lobstr.io/v1/runs"
        payload = {"squid": self.squid_id}
        print("Starting run...")
        resp = requests.post(url, headers=self.headers, json=payload)
        if not resp.ok:
            sys.exit(f"Error starting run: {resp.text}")
        self.run_id = resp.json().get("id")
        if not self.run_id:
            sys.exit("Run ID not found!")
        print("Run started with ID:", self.run_id)

    def poll_run_progress(self):
        url = f"https://api.lobstr.io/v1/runs/{self.run_id}/stats"
        print("Polling for run progress:")
        while True:
            resp = requests.get(url, headers=self.headers)
            if not resp.ok:
                sys.exit(f"Error retrieving run stats: {resp.text}")
            data = resp.json()
            percent_done = data.get("percent_done", "0%")
            results_done = data.get("results_done", 0)
            results_total = data.get("results_total", 0)
            sys.stdout.write(f"\rProgress: {percent_done} ({results_done}/{results_total} results)")
            sys.stdout.flush()
            if data.get("is_done"):
                print("\nRun is complete.")
                break
            time.sleep(2)

    def poll_export_status(self):
        url = f"https://api.lobstr.io/v1/runs/{self.run_id}"
        print("Polling for export completion (export_done:true):")
        max_wait = 120
        interval = 5
        elapsed = 0
        while elapsed < max_wait:
            resp = requests.get(url, headers=self.headers)
            if not resp.ok:
                sys.exit(f"Error retrieving run details: {resp.text}")
            data = resp.json()
            if data.get("export_done", False):
                print("Export is complete.\n")
                print("Run Details:")
                print("Status:", data.get("status"))
                print("Done Reason:", data.get("done_reason"))
                print("Duration:", data.get("duration"))
                print("Credit Used:", data.get("credit_used"))
                print("Total Results:", data.get("total_results"))
                print("Unique Results:", data.get("total_unique_results"))
                return
            print("Export not done yet. Waiting...")
            time.sleep(interval)
            elapsed += interval
        sys.exit("Export did not complete within expected time.")

    def get_s3_url(self):
        url = f"https://api.lobstr.io/v1/runs/{self.run_id}/download"
        print("Requesting download URL for run results...")
        resp = requests.get(url, headers=self.headers)
        if not resp.ok:
            sys.exit(f"Error requesting download URL: {resp.text}")
        s3_url = resp.json().get("s3")
        if not s3_url:
            sys.exit("S3 URL not found!")
        print("\nS3 URL for run results:")
        print(s3_url)
        return s3_url

    def download_csv(self, s3_url):
        print("Downloading CSV file from S3 URL...")
        resp = requests.get(s3_url)
        if not resp.ok:
            sys.exit(f"Error downloading CSV file: {resp.text}")
        filename = "run_results.csv"
        with open(filename, "wb") as f:
            f.write(resp.content)
        print(f"CSV file downloaded and saved as '{filename}'.")

def parse_args():
    parser = argparse.ArgumentParser(description="Seloger API Script")
    parser.add_argument("-c", "--concurrency", type=int, default=1,
                        help="Set the concurrency level for the run (default: 1)")
    parser.add_argument("-a", "--annonce_details", action="store_true",
                        help="Include annonce_details (default: False)")
    parser.add_argument("-l", "--tasks_file", type=str,
                        help="Path to CSV/TSV file containing tasks to upload")
    parser.add_argument("-p", "--max_pages", type=int, default=2,
                        help="Maximum pages for the run (default: 2)")
    return parser.parse_args()

def main():
    args = parse_args()
    api = SelogerAPI(concurrency=args.concurrency, 
                     annonce_details=args.annonce_details, 
                     tasks_file=args.tasks_file, 
                     max_pages=args.max_pages)
    api.create_squid()
    api.update_squid()
    if api.tasks_file:
        task_upload_id = api.upload_tasks_file()
        api.poll_task_upload_status(task_upload_id)
    else:
        api.delete_squid()
        sys.exit("No tasks file provided. Exiting.")
    api.start_run()
    api.poll_run_progress()
    api.poll_export_status()
    s3_url = api.get_s3_url()
    api.download_csv(s3_url)

if __name__ == "__main__":
    main()
