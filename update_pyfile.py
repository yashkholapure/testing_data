import os
import requests

print("Starting script execution...")

# Define your Databricks details
DATABRICKS_HOST = 'https://adb-5219522611471112.12.azuredatabricks.net'
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # Fetch from GitHub repository secrets

# Path to your notebooks in Databricks workspace
DATABRICKS_NOTEBOOK_PATH = '/Workspace/Repos/git_checking/testing_data/'

def update_databricks(notebook_name, notebook_content):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    data = {
        'path': f"{DATABRICKS_NOTEBOOK_PATH}/{notebook_name}",
        'content': notebook_content,
        'overwrite': 'true'
    }
    response = requests.post(url, headers=headers, json=data)
    return response.status_code

def detect_and_update_modified_notebooks():
    changed_files = os.getenv('GITHUB_WORKSPACE')  # GitHub workspace directory
    for root, dirs, files in os.walk(changed_files + '/testing_data'):
        for file in files:
            if file.endswith('.py'):
                notebook_name = file
                notebook_content = open(os.path.join(root, file), 'rb').read()
                status_code = update_databricks(notebook_name, notebook_content)
                if status_code == 200:
                    print(f"Notebook {notebook_name} updated successfully in Databricks.")
                else:
                    print(f"Failed to update {notebook_name} in Databricks.")

if __name__ == "__main__":
    detect_and_update_modified_notebooks()
