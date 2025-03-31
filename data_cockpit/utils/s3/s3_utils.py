import boto3

def list_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    return [bucket['Name'] for bucket in response['Buckets']]

def list_files(bucket, prefix='', delimiter=''):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
    if delimiter:
        operation_parameters['Delimiter'] = delimiter
    page_iterator = paginator.paginate(**operation_parameters)
    files = []
    for page in page_iterator:
        if 'Contents' in page:
            files.extend([content['Key'] for content in page['Contents'] if not content['Key'].endswith('/')])
        if 'CommonPrefixes' in page:
            files.extend([prefix['Prefix'] for prefix in page['CommonPrefixes']])
    return files

def parse_s3_uri(s3_uri):
    if not s3_uri.startswith('s3://'):
        raise ValueError("Invalid S3 URI")
    parts = s3_uri[5:].split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    return bucket, prefix