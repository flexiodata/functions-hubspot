
# ---
# name: hubspot-list-deals
# deployed: true
# title: HubSpot Deals List
# description: Returns a list of deals from HubSpot
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#     required: false
# examples:
# notes: |
#   The following properties are available:
#     * `portal_id`: the portal id for the deal
#     * `deal_id`: the deal id for the deal
#     * `deal_name`: Deal name
#     * `deal_owner`: Deal owner
#     * `deal_state`: Deal stage
#     * `deal_type`: Deal type
#     * `amt`: Amount
#     * `amt_home`: Amount in home currency
#     * `closed_lost_reason`: Closed lost reason
#     * `closed_won_reason`: Closed won reason
#     * `close_date`: Close date
#     * `description`: Deal description
#     * `pipeline`: Pipeline
#     * `contacts_cnt`: Number of contacts
#     * `sales_activities_cnt`: Number of sales activities
#     * `times_contacted_cnt`: Number of times contacted
#     * `last_contacted_date`: Last contacted date
#     * `next_activity_date`: Next activity date
#     * `created_date`: Created date
#     * `updated_date`: Last activity date
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
    auth_token = dict(flex.vars).get('hubspot_connection',{}).get('access_token')
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
    def convertTimestamp(ts):
        if ts is None:
            return ''
        return datetime.utcfromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S')
    property_map = OrderedDict()
    property_map['portal_id'] = lambda item: str(item.get('portalId',''))
    property_map['deal_id'] = lambda item: str(item.get('dealId',''))
    property_map['deal_name'] = lambda item: item.get('properties',{}).get('dealname',{}).get('value','')
    property_map['deal_owner'] = lambda item: str(item.get('properties',{}).get('hubspot_owner_id',{}).get('value',''))
    property_map['deal_state'] = lambda item: item.get('properties',{}).get('dealstage',{}).get('value','')
    property_map['deal_type'] = lambda item: item.get('properties',{}).get('dealtype',{}).get('value','')
    property_map['amt'] = lambda item: item.get('properties',{}).get('amount',{}).get('value','')
    property_map['amt_home'] = lambda item: item.get('properties',{}).get('amount_in_home_currency',{}).get('value','')
    property_map['closed_lost_reason'] = lambda item: item.get('properties',{}).get('closed_lost_reason',{}).get('value','')
    property_map['closed_won_reason'] = lambda item: item.get('properties',{}).get('closed_won_reason',{}).get('value','')
    property_map['close_date'] = lambda item: convertTimestamp(item.get('properties',{}).get('closedate',{}).get('value',None))
    property_map['description'] = lambda item: item.get('properties',{}).get('description',{}).get('value','')
    property_map['pipeline'] = lambda item: item.get('properties',{}).get('pipeline',{}).get('value','')
    property_map['contacts_cnt'] = lambda item: item.get('properties',{}).get('num_associated_contacts',{}).get('value','')
    property_map['sales_activities_cnt'] = lambda item: item.get('properties',{}).get('num_notes',{}).get('value','')
    property_map['times_contacted_cnt'] = lambda item: item.get('properties',{}).get('num_contacted_notes',{}).get('value','')
    property_map['last_contacted_date'] = lambda item: convertTimestamp(item.get('properties',{}).get('notes_last_contacted',{}).get('value',None))
    property_map['next_activity_date'] = lambda item: convertTimestamp(item.get('properties',{}).get('notes_next_activity_date',{}).get('value',None))
    property_map['created_date'] = lambda item: convertTimestamp(item.get('properties',{}).get('createdate',{}).get('value',None))
    property_map['updated_date'] = lambda item: convertTimestamp(item.get('properties',{}).get('notes_last_updated',{}).get('value',None))

    # list of this function's properties we'd like to query
    properties = [p.lower().strip() for p in input['properties']]

    # if we have a wildcard, get all the properties
    if len(properties) == 1 and properties[0] == '*':
        properties = list(property_map.keys())

    # map the list of requested properties to hubspot properties; if none are
    # available, include a blank placeholder
    mapped_properties = [property_map.get(p, lambda item: '') for p in properties]

    # get the results
    result = []
    result.append(properties)

    cursor_id = None
    page_idx, page_max = 0, 100
    while True:

        page_result = getTablePage(auth_token, mapped_properties, cursor_id)
        cursor_id = page_result['cursor']
        result += page_result['data']

        page_idx = page_idx + 1
        if page_idx >= page_max or cursor_id is None:
            break

    # return the results
    result = json.dumps(result, default=to_string)
    flex.output.content_type = "application/json"
    flex.output.write(result)

def getTablePage(auth_token, properties, cursor_id):

    # see here for more info:
    # https://developers.hubspot.com/docs/methods/deals/get-all-deals

    try:

        headers = {
            'Authorization': 'Bearer ' + auth_token,
        }
        url_query_params = {
            'limit': 250
        }
        if cursor_id is not None:
            url_query_params['offset'] = cursor_id

        url_query_str = urllib.parse.urlencode(url_query_params)
        properties_str = "&properties=" + "&properties=".join([
            'dealname',
            'hubspot_owner_id',
            'dealstage',
            'dealtype',
            'amount',
            'amount_in_home_currency',
            'closed_lost_reason',
            'closed_won_reason',
            'closedate',
            'description',
            'pipeline',
            'num_associated_contacts',
            'num_notes',
            'num_contacted_notes',
            'notes_last_contacted',
            'notes_next_activity_date',
            'createdate',
            'notes_last_updated'
            ])
        url = 'https://api.hubapi.com/deals/v1/deal/paged?' + url_query_str + properties_str

        # get the response
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        content = response.json()

        # get the data and the next cursor
        data = []
        results = content.get('deals',[])

        for result_info in results:
            row = [p(result_info) or '' for p in properties]
            data.append(row)

        has_more = content.get('hasMore', False)
        next_cursor_id = content.get('offset')
        if has_more is False:
            next_cursor_id = None

        return {"data": data, "cursor": next_cursor_id}

    except:
        return {"data": [], "cursor": None}

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
