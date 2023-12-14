import requests
import os

# Define your Databricks details
DATABRICKS_HOST = 'https://adb-5219522611471112.12.azuredatabricks.net'
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')  # Fetch from GitHub repository secrets

def list_items_in_workspace(path='/'):
    url = f"{DATABRICKS_HOST}/api/2.0/workspace/list"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }
    data = {'path': path}

    try:
        response = requests.get(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an error for HTTP errors (status codes >= 400)
        items_list = [item["path"] for item in response.json().get("objects", [])]
        return items_list
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Detailed error message: {response.text}")  # Print detailed error message from the API
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

if __name__ == "__main__":
    workspace_items = list_items_in_workspace()
    if workspace_items:
        print("Items in the workspace:")
        for item in workspace_items:
            print(item)
