import requests
import yaml
import json

def parse_s3_arn(arn):
    """
    Converts an S3 ARN to a standard S3 URI.
    Example:
        Input: arn:aws:s3:::bucket-name/path/
        Output: s3://bucket-name/path/
    """
    if arn.startswith("arn:aws:s3:::"):
        s3_uri = "s3://" + arn.replace("arn:aws:s3:::", "")
        # Ensure it ends with '/'
        if not s3_uri.endswith("/"):
            s3_uri += "/"
        return s3_uri
    else:
        return None

def fetch_public_datasets():
    """
    Retrieves all datasets from the AWS Open Data Registry, excludes deprecated ones,
    and returns a dictionary with the specified structure.
    """
    # GitHub API URL to list files in the 'datasets' directory
    api_url = "https://api.github.com/repos/awslabs/open-data-registry/contents/datasets"

    headers = {
        'Accept': 'application/vnd.github.v3+json'
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Ensure the request was successful
    except requests.exceptions.RequestException as e:
        print(f"Error accessing GitHub API: {e}")
        return {}

    datasets = response.json()
    public_datasets_dict = {}

    for dataset in datasets:
        # Only process YAML files
        if dataset['name'].endswith(('.yaml', '.yml')):
            dataset_url = dataset['download_url']
            try:
                dataset_response = requests.get(dataset_url)
                dataset_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error downloading {dataset['name']}: {e}")
                continue

            try:
                dataset_yaml = yaml.safe_load(dataset_response.text)
            except yaml.YAMLError as e:
                print(f"Error parsing YAML from {dataset['name']}: {e}")
                continue

            # Check if marked as Deprecated
            if dataset_yaml.get('Deprecated', False):
                print(f"Dataset '{dataset_yaml.get('Name', 'Unnamed')}' is deprecated. It will be skipped.")
                continue

            name = dataset_yaml.get('Name')
            documentation = dataset_yaml.get('Documentation') or dataset_yaml.get('url')  # Adjust based on structure
            resources = dataset_yaml.get('Resources', [])
            tags = dataset_yaml.get('Tags', [])  # Extract tags

            # Extract the ARN of the 'S3 Bucket' resource
            s3_arn = None
            for resource in resources:
                if resource.get('Type') == 'S3 Bucket':
                    s3_arn = resource.get('ARN')
                    break

            if not name:
                print(f"The dataset in {dataset['name']} does not have a 'Name' field. It will be skipped.")
                continue

            if not s3_arn:
                print(f"The dataset '{name}' does not have an 'S3 Bucket' type resource. It will be skipped.")
                continue

            s3_prefix = parse_s3_arn(s3_arn)
            if not s3_prefix:
                print(f"The ARN '{s3_arn}' for dataset '{name}' is invalid or not of type S3. It will be skipped.")
                continue

            if not documentation:
                # Attempt to get an alternative URL if no documentation is available
                documentation = dataset_yaml.get('Contact')  # Or adjust based on structure

            if not documentation:
                print(f"The dataset '{name}' does not have an associated URL. It will be skipped.")
                continue

            # Add to the dictionary
            public_datasets_dict[name] = {
                "prefix": s3_prefix,
                "url": documentation,
                "tags": tags  # Add tags
            }

    return public_datasets_dict

def format_public_datasets_dict(datasets_dict):
    """
    Formats the datasets dictionary for better readability.
    """
    formatted_dict = "public_datasets_dict = {\n"
    for name, info in datasets_dict.items():
        formatted_dict += f'    "{name}": {{\n'
        formatted_dict += f'        "prefix": "{info["prefix"]}",\n'
        formatted_dict += f'        "url": "{info["url"]}",\n'
        formatted_dict += f'        "tags": {info["tags"]}\n'
        formatted_dict += "    },\n"
    formatted_dict += "}"
    return formatted_dict

if __name__ == "__main__":
    public_datasets_dict = fetch_public_datasets()

    if public_datasets_dict:
        formatted_output = format_public_datasets_dict(public_datasets_dict)
        print(formatted_output)

        # Optional: Save the dictionary to a JSON file
        with open('../data/public_datasets_dict.json', 'w') as f:
            json.dump(public_datasets_dict, f, indent=4)
    else:
        print("No valid public datasets found.")
