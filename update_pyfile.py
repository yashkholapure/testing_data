import os
import requests
import base64

print("Starting script execution...")

# Define your Databricks details
DATABRICKS_HOST = 'https://adb-5219522611471112.12.azuredatabricks.net'
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # Fetch from GitHub repository secrets

# Path to your notebooks in Databricks workspace
DATABRICKS_NOTEBOOK_PATH = '/Repos/git_checking/testing_data'

def delete_databricks_file(notebook_path):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/delete"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    data = {
        'path': notebook_path,
        'recursive': False
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

def update_databricks(notebook_name, notebook_content):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/import"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    notebook_content_base64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
    data = {
        'path': f"{DATABRICKS_NOTEBOOK_PATH}/{notebook_name}",
        'content': notebook_content_base64,
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

def detect_and_update_modified_notebooks():
    changed_files = os.getenv('GITHUB_WORKSPACE')
    print("outside loop...")

    for root, dirs, files in os.walk(changed_files + '/testing'):
        print("inside first loop...")

        for file in files:
            print("inside second loop...")
            if file.endswith('.py'):
                print("inside if statement...")
                notebook_name = file
                notebook_content = open(os.path.join(root, file), 'r').read()

                # Get the notebook path to delete
                notebook_path = f"{DATABRICKS_NOTEBOOK_PATH}/{notebook_name}"

                # Delete the notebook from Databricks
                delete_status = delete_databricks_file(notebook_path)
                if delete_status == 200:
                    print(f"Deleted {notebook_name} from Databricks.")

                # Upload the updated notebook content
                status_code = update_databricks(notebook_name, notebook_content)
                if status_code == 200:
                    print(f"Notebook {notebook_name} updated successfully in Databricks.")
                else:
                    print(f"Failed to update {notebook_name} in Databricks. Status code: {status_code}")

if __name__ == "__main__":
    detect_and_update_modified_notebooks()
