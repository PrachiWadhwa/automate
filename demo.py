import requests
import json
import os

# Configuration
api_base_url = "https://api.au0.signalfx.com"
api_token = "JqjrwXyfdntdeAHBq1vNJQ"
metrics_query_endpoint = "/v2/metric"
new_relic_url = "https://metric-api.newrelic.com/metric/v1"
new_relic_api_key = "12138cc2cd0b78745bfdac762276329cFFFFNRAL""

# File to persist sent timestamps
timestamp_file = "sent_timestamps.txt"

def load_sent_timestamps():
    """Load the set of sent timestamps from a text file."""
    if os.path.exists(timestamp_file):
        with open(timestamp_file, 'r') as file:
            timestamps = set()
            for line in file:
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                try:
                    # Convert each line to a float and add it to the set
                    timestamp = float(line)
                    timestamps.add(timestamp)
                except ValueError:
                    # Print an error message and continue if conversion fails
                    print(f"Warning: Could not convert line to float: '{line}'")
            return timestamps

    return set()



def save_sent_timestamps(new_timestamps):
    """Append new timestamps to a text file and print it to the console."""
    # Convert the set to a list of strings for text serialization
    timestamps_list = [str(ts) for ts in new_timestamps]
    print("newtimestamplist")
    print(timestamps_list)
    # Print the content to the console
    print("Appending the following timestamps to the file:")
    print("\n".join(timestamps_list))  # Print each timestamp on a new line
    
    # Append the new timestamps to the file
    with open(timestamp_file, 'a') as file:
        file.write("\n".join(timestamps_list))

def fetch_metrics():
    """Fetch metrics from the SignalFx API."""
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
        print("Fetched data from SignalFx:", json.dumps(data, indent=4))
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
    """Transform the metrics data to the format required by New Relic."""
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
    """Send the transformed metrics data to New Relic."""
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
    """Main function to orchestrate the process."""
    # Load existing timestamps
    sent_timestamps = load_sent_timestamps()

     # Fetch new metrics
    data = fetch_metrics()

    if data:
        # Transform metrics data
        metrics_data = transform_metrics(data)
        
        if metrics_data:
            # Filter out metrics with timestamps that have already been sent
            new_metrics = [metric for metric in metrics_data if metric["metrics"][0]["timestamp"] not in sent_timestamps]
            
            if new_metrics:
                print("\n----------------------------\n")
                print("Transformed metrics data:", json.dumps(new_metrics, indent=4))
                print("\n----------------------------\n")
                
                # Send new metrics to New Relic
                send_to_new_relic(new_metrics)
                
                # Update and persist timestamps of sent metrics
                new_timestamps = {metric["metrics"][0]["timestamp"] for metric in new_metrics}
              
                
                # Append the new timestamps to the file
                save_sent_timestamps(new_timestamps)
            else:
                print("No new metrics to send.")
 
if __name__ == "__main__":
    main()
