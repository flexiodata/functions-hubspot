
# ---
# name: hubspot-list-deals
# deployed: true
# title: HubSpot Deals List
# description: Returns a list of deals from HubSpot
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all columns). See "Notes" for a listing of the available properties.
#     required: false
# examples:
# notes: |
#   The following properties are allowed:
#     * `amt`: Amount
#     * `amt_home`: Amount in home currency
#     * `close_date`: Close date
#     * `closed_lost_reason`: Closed lost reason
#     * `closed_won_reason`: Closed won reason
#     * `description`: Deal description
#     * `deal_name`: Deal name
#     * `deal_owner`: Deal owner
#     * `deal_state`: Deal stage,
#     * `deal_type`: Deal type,
#     * `pipeline`: Pipeline
#     * `created_date`: Created date
#     * `contacts_cnt`: Number of contacts
#     * `sales_activities_cnt`: Number of sales activities
#     * `times_contacted_cnt`: Number of times contacted
#     * `last_updated_date`: Last activity date
#     * `last_contacted_date`: Last contacted date
#     * `next_activity_date`: Next activity date
# ---

import json
import requests
import urllib
import itertools
from datetime import *
from cerberus import Validator
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    # get the api key from the variable input
    auth_token = dict(flex.vars).get('hubspot_api_key')
    if auth_token is None:
        flex.output.content_type = "application/json"
        flex.output.write([[""]])
        return

    # get the input
    input = flex.input.read()
    try:
        input = json.loads(input)
        if not isinstance(input, list): raise ValueError
    except ValueError:
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    # if the input is valid return an error
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    # map this function's property names to the API's property names
    property_map = OrderedDict()
    property_map['deal_name'] = 'dealname'
    property_map['deal_owner'] = 'hubspot_owner_id'
    property_map['deal_state'] = 'dealstage'
    property_map['deal_type'] = 'dealtype'
    property_map['amt'] = 'amount'
    property_map['amt_home'] = 'amount_in_home_currency'
    property_map['closed_lost_reason'] = 'closed_lost_reason'
    property_map['closed_won_reason'] = 'closed_won_reason'
    property_map['close_date'] = 'closedate'
    property_map['description'] = 'description'
    property_map['pipeline'] = 'pipeline'
    property_map['contacts_cnt'] = 'num_associated_contacts'
    property_map['sales_activities_cnt'] = 'num_notes'
    property_map['times_contacted_cnt'] = 'num_contacted_notes'
    property_map['created_date'] = 'createdate'
    property_map['updated_date'] = 'notes_last_updated'
    property_map['last_contacted_date'] = 'notes_last_contacted'
    property_map['next_activity_date'] = 'notes_next_activity_date'

    try:

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the HubSpot properties we'd like to query
        hubspot_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://developers.hubspot.com/docs/methods/deals/get-all-deals
        url_query_params = {
            'hapikey': auth_token,
            'limit': 100,
            'properties': ''
        }
        url_query_str = urllib.parse.urlencode(url_query_params)
        properties_str = "&properties=".join(hubspot_properties)
        url = 'https://api.hubapi.com/deals/v1/deal/paged?' + url_query_str + properties_str

        # get the response data as a JSON object
        response = requests.get(url)
        content = response.json()

        # return the info
        result = []
        result.append(properties)

        deals = content.get('deals',[])
        for contact in deals:
            row = []
            for p in hubspot_properties:
                row.append(contact.get('properties').get(p,{}).get('value','') or '')
            result.append(row)

        # return the results
        result = json.dumps(result, default=to_string)
        flex.output.content_type = "application/json"
        flex.output.write(result)

    except:
        raise RuntimeError

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None
