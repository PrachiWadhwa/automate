

import requests
import json
import os

# Configuration
api_base_url = "https://api.au0.signalfx.com"
api_token ="JqjrwXyfdntdeAHBq1vNJQ"
metrics_query_endpoint = "/v2/metric"
new_relic_url = "https://metric-api.newrelic.com/metric/v1"
new_relic_api_key = "10f9a85486e1a62e0b74726098445c8cFFFFNRAL"

# File to persist sent timestamps
timestamp_file = "sent_timestamps.json"

def load_sent_timestamps():
    if os.path.exists(timestamp_file):
        with open(timestamp_file, 'r') as file:
            try:
                return set(json.load(file))
            except json.JSONDecodeError:
                # If file is empty or not valid JSON, return an empty set
                return set()
    return set()

def save_sent_timestamps(timestamps):
    with open(timestamp_file, 'w') as file:
        json.dump(list(timestamps), file)

def fetch_metrics():
    url = f"{api_base_url}{metrics_query_endpoint}"
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }
    query_params = {}

    try:
        response = requests.get(url, headers=headers, params=query_params)
        response.raise_for_status()
        data = response.json()
        print("Fetched data from Splunk:", json.dumps(data, indent=4))
        return data
    except requests.exceptions.HTTPError as http_err:
        print('HTTP error occurred:', http_err)
        print('Response content:', response.text)
    except requests.exceptions.RequestException as req_err:
        print('Request error occurred:', req_err)
    except ValueError as json_err:
        print('JSON decoding error occurred:', json_err)
    return None

def transform_metrics(data):
    transformed = []
    for metric in data.get('results', []):
        timestamp = metric.get('created', 0) / 1000
        transformed.append({
            "metrics": [
                {
                    "name": metric.get('name'),
                    "type": metric.get('type', '').lower(),
                    "value": 0,
                    "timestamp": timestamp
                }
            ]
        })
    return transformed

def send_to_new_relic(metrics_data):
    headers = {
        'Api-Key': new_relic_api_key,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(new_relic_url, headers=headers, json=metrics_data)
        response.raise_for_status()
        print("Data sent to New Relic successfully. Response:", response.text)
    except requests.exceptions.HTTPError as http_err:
        print('HTTP error occurred while sending data to New Relic:', http_err)
        print('Response content:', response.text)
    except requests.exceptions.RequestException as req_err:
        print('Request error occurred while sending data to New Relic:', req_err)

def main():
    sent_timestamps = load_sent_timestamps()
    
    data = fetch_metrics()

    if data:
        metrics_data = transform_metrics(data)
        
        if metrics_data:
            # Filter out metrics with timestamps that have already been sent
            new_metrics = [metric for metric in metrics_data if metric["metrics"][0]["timestamp"] not in sent_timestamps]
            
            if new_metrics:
                print("\n----------------------------\n")
                print("Transformed metrics data:", json.dumps(new_metrics, indent=4))
                print("\n----------------------------\n")
                
                send_to_new_relic(new_metrics)
                
                # Update and persist timestamps of sent metrics
                for metric in new_metrics:
                    sent_timestamps.add(metric["metrics"][0]["timestamp"])
                
                save_sent_timestamps(sent_timestamps)
            else:
                print("No new metrics to send.")

if __name__ == "__main__":
    main()

