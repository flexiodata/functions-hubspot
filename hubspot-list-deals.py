
# ---
# name: hubspot-list-deals
# deployed: true
# config: index
# title: HubSpot Deals List
# description: Returns a list of deals from HubSpot
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Returns" for a listing of the available properties.
#     required: false
#   - name: filter
#     type: string
#     description: Filter to apply with key/values specified as a URL query string where the keys correspond to the properties to filter.
#     required: false
# returns:
#   - name: portal_id
#     type: string
#     description: The portal id for the deal
#   - name: deal_id
#     type: string
#     description: The deal id for the deal
#   - name: deal_name
#     type: string
#     description: The deal name
#   - name: deal_owner
#     type: string
#     description: The deal owner
#   - name: deal_state
#     type: string
#     description: The deal stage
#   - name: deal_type
#     type: string
#     description: The deal type
#   - name: amt
#     type: string
#     description: The deal mount
#   - name: amt_home
#     type: string
#     description: The deal amount in home currency
#   - name: closed_lost_reason
#     type: string
#     description: The closed lost reason
#   - name: closed_won_reason
#     type: string
#     description: The closed won reason
#   - name: close_date
#     type: string
#     description: The close date
#   - name: description
#     type: string
#     description: The deal description
#   - name: pipeline
#     type: string
#     description: The pipeline
#   - name: contacts_cnt
#     type: string
#     description: The number of contacts
#   - name: sales_activities_cnt
#     type: string
#     description: The number of sales activities
#   - name: times_contacted_cnt
#     type: string
#     description: The number of times contacted
#   - name: last_contacted_date
#     type: string
#     description: The last contacted date
#   - name: next_activity_date
#     type: string
#     description: The next activity date
#   - name: created_date
#     type: string
#     description: The created date
#   - name: updated_date
#     type: string
#     description: The last activity date
# examples:
# ---

import json
import requests
import urllib
from datetime import *
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    # get the api key from the variable input
    auth_token = dict(flex.vars).get('hubspot_connection',{}).get('access_token')
    if auth_token is None:
        flex.output.content_type = "application/json"
        flex.output.write([[""]])
        return

    # map this function's property names to the API's property names
    def convertTimestamp(ts):
        if ts is None or ts == '':
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
    properties = list(property_map.keys())

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
    page_idx, page_max = 0, 1000
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

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value
