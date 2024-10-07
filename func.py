#
# oci-monitoring-metrics-to-splunk-observability version 0.1.
#
# Copyright (c) 2024, Splunk, Inc. All rights reserved.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

import io
import json
import logging
import os

import requests

"""
This sample OCI Function maps OCI Monitoring Service Metrics to the Splunk
Observability REST API "v2/datapoint" contract found here:

https://dev.splunk.com/observability/reference/api/ingest_data/latest#endpoint-send-metrics

"""

# Use OCI Application or Function configurations to override these environment variable defaults.

api_token = os.getenv('SPLUNK_ACCESS_TOKEN', 'not-configured')
api_realm = os.getenv('SPLUNK_REALM', 'us0')
is_forwarding = eval(os.getenv('FORWARD_TO_SPLUNK', 'True'))
is_tracing = eval(os.getenv('ENABLE_TRACING', 'False'))

logging_level = os.getenv('LOGGING_LEVEL', 'INFO')
logging.basicConfig(level=logging_level)
logger = logging.getLogger()

# Constants

X_SF_TOKEN_HEADER = 'X-SF-Token'

# Global session reused between invocations

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
session.mount('https://', adapter)


# Functions

def handler(ctx, data: io.BytesIO = None):
    """
    OCI Function Entry Point
    :param ctx: InvokeContext
    :param data: data payload
    :return: plain text response indicating success or error
    """

    try:
        metric_records = json.loads(data.getvalue())
        logger.info(f'metrics count: {len(metric_records)} log level: {logging_level} forwarding to Splunk: {is_forwarding}')
        logger.debug(metric_records)
        splunk_metrics = convert(metric_records)
        send_to_splunk_o11y(splunk_metrics)

    except Exception as ex:
        logger.error(f'error handling logging payload: {str(ex)}')
        logger.error(ex)


def convert(metric_records) -> list:
    """
    :param metric_records: the list of OCI metric records.
    :return: the list of Splunk Observability records
    """

    result = []
    for metric_record in metric_records:
        splunk_datapoints = transform_metric_to_splunk_o11y_datapoints(metric_record)
        result.extend(splunk_datapoints)
        logger.debug(splunk_datapoints)

    return result


def transform_metric_to_splunk_o11y_datapoints(metric_record: dict) -> list:
    """
    Transform metrics to Splunk Observability format. OCI does not define metric
    types, so all OCI metrics are presented as gauge type.
    See: https://dev.splunk.com/observability/reference/api/ingest_data/latest#endpoint-send-metrics
    :param metric_record: metric record
    :return: Splunk Observability json datapoint record
    """

    o11y_dps = []
    datapoints = get_dictionary_value(dictionary=metric_record, key='datapoints')
    metric_name = get_dictionary_value(metric_record, 'name')
    metric_dims = get_metric_dimensions(metric_record)
    for point in datapoints:
        o11y_dp = {
            'metric': metric_name,
            'value': point.get('value'),
            'dimensions': metric_dims,
            'timestamp': point.get('timestamp')
        }
        o11y_dps.append(o11y_dp)

    ordered_dps = sorted(o11y_dps, key=lambda dp: dp['timestamp'])
    return ordered_dps


def get_metric_dimensions(metric_record: dict) -> dict:
    """
    Assembles dimensions from selected OCI metric attributes.
    :param metric_record: the metric record to scan
    :return: dictionary of dimensions meeting Splunk Observability semantics
    """

    result = {}
    result['oci_namespace'] = get_dictionary_value(dictionary=metric_record, key='namespace')
    result['oci_compartment_id'] = get_dictionary_value(dictionary=metric_record, key='compartmentId')

    unit = get_dictionary_value(dictionary=metric_record['metadata'], key='unit')
    if unit is not None:
        result['oci_unit'] = unit
    rg = get_dictionary_value(dictionary=metric_record, key='resourceGroup')
    if rg is not None:
        result['oci_resource_group'] = rg

    dim_dict = get_dictionary_value(dictionary=metric_record, key='dimensions')
    for dim in dim_dict.items():
        if fix_dimension_value(dim[1]) is not None:
            result[fix_dimension_name('oci_dim_' + str(dim[0]))] = fix_dimension_value(dim[1])

    return result


def fix_dimension_name(name) -> str:
    no_whitespace = ((str(name).strip()).replace(' ', '_'))
    no_lead_underscores = no_whitespace.lstrip('_')
    no_quotes = (no_lead_underscores.replace('\"', '_')).replace('\'', '_')
    not_too_long = no_quotes[:128]
    return not_too_long


def fix_dimension_value(value) -> str:
    no_whitespace = (str(value)).strip()
    no_quotes = (no_whitespace.replace('\"', '_')).replace('\'', '_')
    not_too_long = no_quotes[:256]
    return not_too_long


def with_obfuscated_sf_token(headers: dict) -> dict:
    result = dict(headers)
    token = result[X_SF_TOKEN_HEADER]
    result[X_SF_TOKEN_HEADER] = token[:4] + '...' + token[-4:]
    return result


def send_to_splunk_o11y(splunk_metrics) -> None:
    """
    Sends metrics to the Splunk Observability "/v2/datapoint" endpoint.
    :param splunk_metrics: list of metrics
    :return: None
    """

    if is_forwarding is False:
        logger.info('Splunk Observability forwarding is disabled - dropping data')
        return

    post_url = f'https://ingest.{api_realm}.signalfx.com/v2/datapoint'
    api_headers = {'Content-Type': 'application/json', X_SF_TOKEN_HEADER: api_token}

    sorted_splunk_metrics = sorted(splunk_metrics, key=lambda dp: dp['timestamp'])
    message_body = {'gauge': sorted_splunk_metrics}

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            f'POST to Splunk Observability: {post_url}\n'
            f'Headers: {json.dumps(with_obfuscated_sf_token(api_headers))}\n'
            f'Content: {json.dumps(message_body)}')
    response = session.post(post_url, data=json.dumps(message_body), headers=api_headers)

    if response.status_code != 200:
        raise Exception(f'Error sending metrics to Splunk Observability: {response.status_code} {response.reason}')


def get_dictionary_value(dictionary: dict, key: str) -> any:
    """
    Recursive method to find value within a dictionary which may also have nested lists / dictionaries.
    :param dictionary: the dictionary to scan
    :param key: the key we are looking for
    :return: If a target_key exists multiple times in the dictionary, the first one found will be returned.
    """

    if dictionary is None:
        return None

    value = dictionary.get(key)
    if value:
        return value

    for _, value in dictionary.items():
        if isinstance(value, dict):
            value = get_dictionary_value(dictionary=value, key=key)
            if value:
                return value

        elif isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    value = get_dictionary_value(dictionary=entry, key=key)
                    if value:
                        return value


def local_test_mode(filename):
    """
    This routine reads a local JSON metrics file, converting the contents to Splunk Observability format.
    :param filename: JSON file exported from OCI Logging UI or CLI.
    :return: None
    """

    with open(filename, 'r') as f:
        for line in f:
            handler({}, io.BytesIO(bytes(line, 'utf-8')))

    logger.info('DONE')


if __name__ == '__main__':
    local_test_mode('oci-metrics-test-file.json')
    
