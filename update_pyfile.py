import os
import requests

# Define your Databricks details
DATABRICKS_HOST = 'https://adb-5219522611471112.12.azuredatabricks.net'
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # Fetch from GitHub repository secrets

def list_databricks_files(notebook_path):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/list"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    data = {
        'path': notebook_path
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        files = response.json()['objects']
        file_names = [file['path'] for file in files if file['object_type'] == 'NOTEBOOK']
        return file_names
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Detailed error message: {response.text}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

if __name__ == "__main__":
    notebook_path = '/Workspace/Repos/git_checking/testing_data/testing'  # Change this to your desired path
    files_list = list_databricks_files(notebook_path)
    if files_list:
        print("List of files in Databricks:")
        for file_name in files_list:
            print(file_name)
