import argparse
import os
import sys
import time
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()


class SelogerAPI:
    """SeLoger Search Export by Lobstr.io"""
    
    def __init__(
        self,
        concurrency: int = 1,
        annonce_details: bool = False,
        tasks_file: Optional[str] = None,
        max_pages: int = 2
    ):
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
        self.squid_id: Optional[str] = None
        self.run_id: Optional[str] = None

    @staticmethod
    def _get_mime_type(file_path: str) -> str:
        """Determine MIME type based on file extension."""
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            return 'text/csv'
        elif ext == '.tsv':
            return 'text/tab-separated-values'
        sys.exit(f"Invalid file extension: {ext}. Valid values: csv/tsv")

    def create_squid(self) -> None:
        """Create a new squid instance for API operations."""
        url = "https://api.lobstr.io/v1/squids/create"
        payload = {"crawler": "78f5839ee4b97c30e67eec391b907dd0"}
        
        print("Creating squid...")
        response = requests.post(url, headers=self.headers, json=payload)
        
        if not response.ok:
            sys.exit(f"Error creating squid: {response.text}")
            
        self.squid_id = response.json().get("id")
        if not self.squid_id:
            sys.exit("Squid ID not found in response!")
            
        print(f"Squid created with ID: {self.squid_id}")

    def update_squid(self) -> None:
        """Update squid configuration with current parameters."""
        url = f"https://api.lobstr.io/v1/squids/{self.squid_id}"
        payload = {
            "concurrency": self.concurrency,
            "export_unique_results": True,
            "no_line_breaks": True,
            "to_complete": False,
            "params": {
                "max_pages": self.max_pages,
                "fill_results_details": {
                    "annonce_details": self.annonce_details
                }
            },
            "accounts": None,
            "run_notify": "on_success"
        }
        
        print("Updating squid configuration...")
        response = requests.post(url, headers=self.headers, json=payload)
        
        if not response.ok:
            sys.exit(f"Error updating squid: {response.text}")
            
        print("Squid configuration updated successfully")

    def upload_tasks_file(self) -> str:
        """Upload tasks file to the created squid."""
        if not self.tasks_file:
            sys.exit("No tasks file provided for upload")
            
        url = f"https://api.lobstr.io/v1/squids/{self.squid_id}/tasks/upload"
        mime_type = self._get_mime_type(self.tasks_file)
        
        try:
            with open(self.tasks_file, 'rb') as f:
                files = [
                    ('file', (os.path.basename(self.tasks_file), f, mime_type))
                ]
                response = requests.post(
                    url,
                    headers={'Authorization': f'Token {self.api_key}'},
                    files=files
                )
        except IOError as e:
            sys.exit(f"File access error: {e}")
            
        if not response.ok:
            sys.exit(f"Upload failed: {response.text}")
            
        task_upload_id = response.json().get("task_id")
        if not task_upload_id:
            sys.exit("Missing task upload ID in response")
            
        print(f"Tasks file uploaded. Upload Task ID: {task_upload_id}")
        return task_upload_id

    def _poll_task_upload_status(self, task_upload_id: str) -> None:
        """Monitor status of file upload task."""
        url = f"https://api.lobstr.io/v1/tasks/upload/{task_upload_id}"
        print("Checking tasks file upload status:")
        
        max_wait = 60
        interval = 5
        elapsed = 0
        
        while elapsed < max_wait:
            response = requests.get(url, headers=self.headers)
            if not response.ok:
                sys.exit(f"Status check failed: {response.text}")
                
            status = response.json().get("state", "").upper()
            print(f"Current status: {status}")
            
            if status == "SUCCESS":
                print("Tasks file processed successfully")
                return
                
            time.sleep(interval)
            elapsed += interval
            
        sys.exit("Tasks processing exceeded maximum wait time")

    def delete_squid(self) -> None:
        """Clean up squid resource."""
        if not self.squid_id:
            return
            
        url = f"https://api.lobstr.io/v1/squids/{self.squid_id}"
        print("No tasks file provided. Deleting squid...")
        
        response = requests.delete(url, headers=self.headers)
        if not response.ok:
            sys.exit(f"Deletion failed: {response.text}")
            
        print(f"Squid deleted: {response.json()}")

    def start_run(self) -> None:
        """Initialize a new API run."""
        url = "https://api.lobstr.io/v1/runs"
        payload = {"squid": self.squid_id}
        
        print("Starting API run...")
        response = requests.post(url, headers=self.headers, json=payload)
        
        if not response.ok:
            sys.exit(f"Run initiation failed: {response.text}")
            
        self.run_id = response.json().get("id")
        if not self.run_id:
            sys.exit("Missing run ID in response")
            
        print(f"Run started with ID: {self.run_id}")

    def _poll_run_progress(self) -> None:
        """Monitor and display run execution progress."""
        if not self.run_id:
            sys.exit("Missing run ID for progress check")
            
        url = f"https://api.lobstr.io/v1/runs/{self.run_id}/stats"
        print("Monitoring run progress:")
        
        while True:
            response = requests.get(url, headers=self.headers)
            if not response.ok:
                sys.exit(f"Progress check failed: {response.text}")
                
            data = response.json()
            done = data.get("results_done", 0)
            total = data.get("results_total", 0)
            percent = data.get("percent_done", "0%")
            
            print(f"\rProgress: {percent} ({done}/{total} results)", end="")
            
            if data.get("is_done"):
                print("\nRun completed successfully")
                break
                
            time.sleep(2)

    def _poll_export_status(self) -> None:
        """Verify export completion and display results."""
        if not self.run_id:
            sys.exit("Missing run ID for export check")
            
        url = f"https://api.lobstr.io/v1/runs/{self.run_id}"
        print("Verifying export completion:")
        
        max_wait = 120
        interval = 5
        elapsed = 0
        
        while elapsed < max_wait:
            response = requests.get(url, headers=self.headers)
            if not response.ok:
                sys.exit(f"Export check failed: {response.text}")
                
            data = response.json()
            if data.get("export_done", False):
                print("\nExport completed with details:")
                print(f"Status: {data.get('status')}")
                print(f"Reason: {data.get('done_reason')}")
                print(f"Duration: {data.get('duration')}")
                print(f"Credits used: {data.get('credit_used')}")
                print(f"Total results: {data.get('total_results')}")
                print(f"Unique results: {data.get('total_unique_results')}")
                return
                
            print("Waiting for export completion...")
            time.sleep(interval)
            elapsed += interval
            
        sys.exit("Export verification timed out")

    def get_results_url(self) -> str:
        """Retrieve S3 URL for run results."""
        if not self.run_id:
            sys.exit("Missing run ID for results URL")
            
        url = f"https://api.lobstr.io/v1/runs/{self.run_id}/download"
        print("Requesting results download URL...")
        
        response = requests.get(url, headers=self.headers)
        if not response.ok:
            sys.exit(f"URL request failed: {response.text}")
            
        s3_url = response.json().get("s3")
        if not s3_url:
            sys.exit("Missing S3 URL in response")
            
        print(f"\nResults available at: {s3_url}")
        return s3_url

    def download_results(self, s3_url: str) -> None:
        """Download results file from S3 URL."""
        print("Downloading results file...")
        response = requests.get(s3_url)
        
        if not response.ok:
            sys.exit(f"Download failed: {response.text}")
            
        filename = "run_results.csv"
        with open(filename, "wb") as f:
            f.write(response.content)
            
        print(f"Results saved to {filename}")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Execute Seloger property search through Lobstr.io API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=1,
        help="Number of concurrent requests"
    )
    parser.add_argument(
        "-a", "--annonce_details",
        action="store_true",
        help="Include detailed property information"
    )
    parser.add_argument(
        "-l", "--tasks_file",
        type=str,
        help="Path to CSV/TSV file containing search parameters"
    )
    parser.add_argument(
        "-p", "--max_pages",
        type=int,
        default=2,
        help="Maximum number of pages to retrieve"
    )
    return parser.parse_args()


def main():
    """Main execution flow."""
    args = parse_args()
    
    api = SelogerAPI(
        concurrency=args.concurrency,
        annonce_details=args.annonce_details,
        tasks_file=args.tasks_file,
        max_pages=args.max_pages
    )
    
    try:
        api.create_squid()
        api.update_squid()
        
        if api.tasks_file:
            upload_id = api.upload_tasks_file()
            api._poll_task_upload_status(upload_id)
        else:
            api.delete_squid()
            sys.exit("No tasks file provided - exiting")
            
        api.start_run()
        api._poll_run_progress()
        api._poll_export_status()
        
        results_url = api.get_results_url()
        api.download_results(results_url)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)


if __name__ == "__main__":
    main()