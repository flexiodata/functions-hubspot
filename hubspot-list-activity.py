
# ---
# name: hubspot-list-activity
# deployed: true
# title: HubSpot Activity List
# description: Returns a list of activity from HubSpot.
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
# returns:
#   - name: engagement_id
#     type: string
#     description: The id for the engagement
#   - name: portal_id
#     type: string
#     description: The portal id for the engagement
#   - name: deal_id
#     type: string
#     description: The deal id for the engagement
#   - name: company_id
#     type: string
#     description: The company id for the engagement
#   - name: type
#     type: string
#     description: The type of the engagement
#   - name: active
#     type: string
#     description: The status of the engagement; true if the engagement is active and false otherwise
#   - name: created_at
#     type: string
#     description: The creation date of the engagement
#   - name: last_updated
#     type: string
#     description: The date the engagement was last updated
# examples:
#   - ' '
#   - '"*"'
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
    def getDeal(item):
            ids = item.get('associations',{}).get('dealIds',[])
            if len(ids) > 0:
                return str(ids[0])
            else:
                return ''
    def getCompany(item):
            ids = item.get('associations',{}).get('companyIds',[])
            if len(ids) > 0:
                return str(ids[0])
            else:
                return ''
    def convertTimestamp(ts):
        if ts is None or ts == '':
            return ''
        return datetime.utcfromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S')
    property_map = OrderedDict()
    property_map['engagement_id'] = lambda item: str(item.get('engagement',{}).get('id',''))
    property_map['portal_id'] = lambda item: str(item.get('engagement',{}).get('portalId',''))
    property_map['deal_id'] = lambda item: getDeal(item)
    property_map['company_id'] = lambda item: getCompany(item)
    property_map['type'] = lambda item: item.get('engagement',{}).get('type','').lower()
    property_map['active'] = lambda item: item.get('engagement',{}).get('active','')
    property_map['created_at'] = lambda item: convertTimestamp(item.get('engagement',{}).get('createdAt',None))
    property_map['last_updated'] = lambda item: convertTimestamp(item.get('engagement',{}).get('lastUpdated',None))
    #property_map['content'] = lambda item: item.get('metadata',{}).get('body','')
    #property_map['metadata'] = lambda item: json.dumps(item.get('metadata',{}))

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
    page_idx, page_max = 0, 10
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
    # https://developers.hubspot.com/docs/methods/engagements/get-all-engagements
    # https://developers.hubspot.com/docs/methods/engagements/engagements-overview
    # note: pagination mechanism different from other api calls; compare deal pagination

    try:

        # make the request
        headers = {
            'Authorization': 'Bearer ' + auth_token,
        }
        url_query_params = {
            'limit': 250
        }
        if cursor_id is not None:
            url_query_params['offset'] = cursor_id

        url_query_str = urllib.parse.urlencode(url_query_params)
        url = 'https://api.hubapi.com/engagements/v1/engagements/paged?' + url_query_str

        # get the response
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        content = response.json()

        # get the data and the next cursor
        data = []
        results = content.get('results',[])

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
