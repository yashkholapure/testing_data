import os
import requests

print("Starting script execution...")

# Define your Databricks details
DATABRICKS_HOST = 'https://adb-5219522611471112.12.azuredatabricks.net'
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # Fetch from GitHub repository secrets

# Path to your notebooks in Databricks workspace
DATABRICKS_NOTEBOOK_PATH = '/Workspace/Repos/git_checking/testing_data/testing'

def update_databricks(notebook_name, notebook_content):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'text/plain'  # Set content type to 'text/plain'
    }
    
    try:
        response = requests.post(url, headers=headers, data=notebook_content)
        response.raise_for_status()  # Raise an error for HTTP errors (status codes >= 400)
        return response.status_code
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Detailed error message: {response.text}")  # Print detailed error message from the API
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

def detect_and_update_modified_notebooks():
    changed_files = os.getenv('GITHUB_WORKSPACE')  # GitHub workspace directory
    print("outside loop...")

    for root, dirs, files in os.walk(changed_files + '/testing'):
        print("inside first loop...")

        for file in files:
            print("inside second loop...")
            if file.endswith('.py'):
                print("inside if statement...")
                notebook_name = file
                notebook_content = open(os.path.join(root, file), 'r').read()
                status_code = update_databricks(notebook_name, notebook_content)
                if status_code == 200:
                    print(f"Notebook {notebook_name} updated successfully in Databricks.")
                else:
                    print(f"Failed to update {notebook_name} in Databricks. Status code: {status_code}")

if __name__ == "__main__":
    detect_and_update_modified_notebooks()
