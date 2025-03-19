import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Replace with your ActiveCampaign API URL and API Key
API_URL = 'https://anahateducation.api-us1.com/api/3'
API_KEY = 'c7b91f45c8c511041ceadcfd6b85f9158f4be5aa263b5c296eb48a69e78c7eb87d463086'

# Define headers
headers = {
    'Api-Token': API_KEY,
    'Content-Type': 'application/json'
}

# Function to fetch all contacts with pagination
def get_all_contacts():
    contacts = []
    endpoint = f'{API_URL}/contacts'
    params = {
        'limit': 100,  # Adjust the limit as needed
        'offset': 0
    }
    while True:
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            contacts.extend(data.get('contacts', []))
            print(f"Fetched {len(data.get('contacts', []))} contacts, total so far: {len(contacts)}")
            if len(data.get('contacts', [])) < params['limit']:
                break
            params['offset'] += params['limit']
        else:
            print(f"Failed to fetch contacts. Status code: {response.status_code}")
            print(response.text)
            break
    return contacts

# Function to fetch activities for a contact
def get_contact_activities(contact_id):
    activities = []
    endpoint = f'{API_URL}/activities'
    params = {
        'contact': contact_id,
        'limit': 100,  # Adjust the limit as needed
        'offset': 0
    }
    while True:
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            activities.extend(data.get('activities', []))
            if len(data.get('activities', [])) < params['limit']:
                break
            params['offset'] += params['limit']
        else:
            print(f"Failed to fetch activities for contact {contact_id}. Status code: {response.status_code}")
            print(response.text)
            break
    return activities

# Function to save all contacts and their activities to a JSON file
def save_contacts_and_activities_to_json(contacts, all_activities):
    data = []

    for contact in contacts:
        contact_data = contact.copy()
        contact_data['activities'] = all_activities.get(contact['id'], [])
        data.append(contact_data)

    json_file = 'all_contacts_activities.json'
    with open(json_file, 'w') as file:
        json.dump(data, file, indent=4)

    print(f"All contacts and activities saved to {json_file}")

# Fetch all contacts
contacts = get_all_contacts()
print(f"Total contacts fetched: {len(contacts)}")

# Fetch activities for each contact concurrently
all_activities = {}
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(get_contact_activities, contact['id']): contact['id'] for contact in contacts}
    for future in as_completed(futures):
        contact_id = futures[future]
        try:
            activities = future.result()
            all_activities[contact_id] = activities
        except Exception as e:
            print(f"Error fetching activities for contact {contact_id}: {e}")

# Save all contacts and activities to a single JSON file
save_contacts_and_activities_to_json(contacts, all_activities)
