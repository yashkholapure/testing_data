import requests
import os

# Define your Databricks details
DATABRICKS_HOST = 'https://adb-5219522611471112.12.azuredatabricks.net'
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # Fetch from GitHub repository secrets
WORKSPACE_PATH = '/Workspace/Repos/git_checking/testing_data/testing'  # Path to your workspace directory

def list_files_in_workspace():
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/list"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    data = {'path': WORKSPACE_PATH}

    try:
        response = requests.get(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an error for HTTP errors (status codes >= 400)
        files_list = [file["path"] for file in response.json().get("objects", []) if file.get("object_type") == "NOTEBOOK"]
        return files_list
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Detailed error message: {response.text}")  # Print detailed error message from the API
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

if __name__ == "__main__":
    file_names = list_files_in_workspace()
    if file_names:
        print("File names in the workspace:")
        for file_name in file_names:
            print(file_name)
