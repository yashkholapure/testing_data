import os
import requests
import base64

print("Starting script execution...")

# Define your Databricks details
DATABRICKS_HOST = 'https://adb-5219522611471112.12.azuredatabricks.net'
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # Fetch from GitHub repository secrets

# Path to your notebooks in Databricks workspace
DATABRICKS_NOTEBOOK_PATH = '/Repos/git_checking/testing_data/testing'

def delete_databricks_file(notebook_path):
    pass

def update_databricks(notebook_name, notebook_content):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    data = {
        'path': f"{DATABRICKS_NOTEBOOK_PATH}/{notebook_name}",
        'content': notebook_content,
        'format': 'SOURCE',
        'overwrite': 'false'
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.status_code
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Detailed error message: {response.text}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

def delete_and_update_files():
    # Function to delete files in Databricks
    # Similar to the one used in the previous code

    # Fetch and upload files from GitHub to Databricks
    github_files_path = os.getenv('GITHUB_WORKSPACE') + '/testing'
    for root, dirs, files in os.walk(github_files_path):
        for file in files:
            if file.endswith('.py'):
                notebook_name = file
                notebook_content = open(os.path.join(root, file), 'r').read()
                status_code = update_databricks(notebook_name, notebook_content)
                if status_code == 200:
                    print(f"Updated {notebook_name} in Databricks.")
                else:
                    print(f"Failed to update {notebook_name} in Databricks. Status code: {status_code}")

if __name__ == "__main__":
    delete_and_update_files()
