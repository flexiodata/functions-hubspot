
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
#   - name: forecast_close_date
#     type: string
#     description: The forecasted close date; this is a placeholder for an example of a custom field
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

    # get the results
    result = []

    cursor_id = None
    page_idx, page_max = 0, 1000
    while True:

        page_result = getTablePage(auth_token, cursor_id)
        cursor_id = page_result['cursor']
        result += page_result['data']

        page_idx = page_idx + 1
        if page_idx >= page_max or cursor_id is None:
            break

    # return the results
    result = json.dumps(result, default=to_string)
    flex.output.content_type = "application/json"
    flex.output.write(result)

def getTablePage(auth_token, cursor_id):

    # see here for more info:
    # https://knowledge.hubspot.com/deals/hubspots-default-deal-properties
    # https://developers.hubspot.com/docs/methods/deals/get-all-deals

    # see here: to get all available deal properties:
    # https://developers.hubspot.com/docs/methods/deals/get_deal_properties
    # example: https://api.hubapi.com/properties/v1/deals/properties?hapikey=demo

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
            'forecast_close_date', # example of custom field
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
        page = content.get('deals',[])

        for item in page:
            row = OrderedDict()
            row['portal_id'] = str(item.get('portalId',''))
            row['deal_id'] = str(item.get('dealId',''))
            row['deal_name'] = item.get('properties',{}).get('dealname',{}).get('value','')
            row['deal_owner'] = str(item.get('properties',{}).get('hubspot_owner_id',{}).get('value',''))
            row['deal_state'] = item.get('properties',{}).get('dealstage',{}).get('value','')
            row['deal_type'] = item.get('properties',{}).get('dealtype',{}).get('value','')
            row['amt'] = to_integer(item.get('properties',{}).get('amount',{}).get('value',''))
            row['amt_home'] = to_integer(item.get('properties',{}).get('amount_in_home_currency',{}).get('value',''))
            row['closed_lost_reason'] = item.get('properties',{}).get('closed_lost_reason',{}).get('value','')
            row['closed_won_reason'] = item.get('properties',{}).get('closed_won_reason',{}).get('value','')
            row['forecast_close_date'] = to_date(item.get('properties',{}).get('closedate',{}).get('value',None)) # example of custom field
            row['close_date'] = to_date(item.get('properties',{}).get('closedate',{}).get('value',None))
            row['description'] = item.get('properties',{}).get('description',{}).get('value','')
            row['pipeline'] = item.get('properties',{}).get('pipeline',{}).get('value','')
            row['contacts_cnt'] = to_integer(item.get('properties',{}).get('num_associated_contacts',{}).get('value',''))
            row['sales_activities_cnt'] = to_integer(item.get('properties',{}).get('num_notes',{}).get('value',''))
            row['times_contacted_cnt'] = to_integer(item.get('properties',{}).get('num_contacted_notes',{}).get('value',''))
            row['last_contacted_date'] = to_date(item.get('properties',{}).get('notes_last_contacted',{}).get('value',None))
            row['next_activity_date'] = to_date(item.get('properties',{}).get('notes_next_activity_date',{}).get('value',None))
            row['created_date'] = to_date(item.get('properties',{}).get('createdate',{}).get('value',None))
            row['updated_date'] = to_date(item.get('properties',{}).get('notes_last_updated',{}).get('value',None))
            data.append(row)

        has_more = content.get('hasMore', False)
        next_cursor_id = content.get('offset')
        if has_more is False:
            next_cursor_id = None

        return {"data": data, "cursor": next_cursor_id}

    except:
        return {"data": [], "cursor": None}

def to_date(ts):
    if ts is None or ts == '':
        return ''
    return datetime.utcfromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S')

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def to_integer(value):
    try:
        return int(value)
    except ValueError:
        return ''

