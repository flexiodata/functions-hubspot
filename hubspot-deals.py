
# ---
# name: hubspot-deals
# deployed: true
# config: index
# title: HubSpot Deals
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
#   - name: deal_stage
#     type: string
#     description: The deal stage
#   - name: deal_type
#     type: string
#     description: The deal type
#   - name: amount
#     type: string
#     description: The deal mount
#   - name: amount_in_home_currency
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
#   - name: num_notes
#     type: string
#     description: The number of notes
#   - name: num_associated_contacts
#     type: string
#     description: The number of associated contacts
#   - name: num_contacted_notes
#     type: string
#     description: The number of contact-related notes
#   - name: notes_last_contacted
#     type: string
#     description: The last contacted date associated with the notes
#   - name: notes_last_updated
#     type: string
#     description: The date the notes were last updated
#   - name: notes_next_activity_date
#     type: string
#     description: The next notes activity date
#   - name: created_at
#     type: string
#     description: The date the deal was added to the system
# examples:
# ---

import json
import urllib
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import *
from collections import OrderedDict

# main function entry point
def flexio_handler(flex):

    flex.output.content_type = 'application/x-ndjson'
    for item in get_data(flex.vars):
        result = json.dumps(item, default=to_string) + "\n"
        flex.output.write(result)

def get_data(params):

    # get the api key from the variable input
    auth_token = dict(params).get('hubspot_connection',{}).get('access_token')

    # see here for more info:
    # https://knowledge.hubspot.com/deals/hubspots-default-deal-properties
    # https://developers.hubspot.com/docs/methods/deals/get-all-deals

    # see here: to get all available deal properties:
    # https://developers.hubspot.com/docs/methods/deals/get_deal_properties
    # example: https://api.hubapi.com/properties/v1/deals/properties?hapikey=demo

    headers = {
        'Authorization': 'Bearer ' + auth_token,
    }
    url = 'https://api.hubapi.com/deals/v1/deal/paged'

    request_properties = [
        'dealname','hubspot_owner_id','dealstage','dealtype','amount','amount_in_home_currency',
        'closed_lost_reason','closed_won_reason','forecast_close_date', # forecast_close_date is example of custom field
        'closedate','description','pipeline','num_associated_contacts','num_notes',
        'num_contacted_notes','notes_last_contacted','notes_next_activity_date','createdate',
        'notes_last_updated'
    ]

    page_size = 250
    page_cursor_id = None
    while True:

        url_query_params = {'limit': page_size}
        if page_cursor_id is not None:
            url_query_params['offset'] = page_cursor_id
        url_query_str = urllib.parse.urlencode(url_query_params)
        url_request_properties = "&properties=" + "&properties=".join(request_properties)

        page_url = url + '?' + url_query_str + url_request_properties
        response = requests_retry_session().get(page_url, headers=headers)
        response.raise_for_status()
        content = response.json()
        data = content.get('deals',[])

        if len(data) == 0 :# sanity check in case there's an issue with cursor
            break

        for item in data:
            yield get_item_info(item)

        has_more = content.get('hasMore', False)
        if has_more is False:
            break

        page_cursor_id = content.get('offset')

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

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

def get_item_info(item):

    info = OrderedDict()

    info['portal_id'] = str(item.get('portalId',''))
    info['deal_id'] = str(item.get('dealId',''))
    info['deal_name'] = item.get('properties',{}).get('dealname',{}).get('value','')
    info['deal_owner'] = str(item.get('properties',{}).get('hubspot_owner_id',{}).get('value',''))
    info['deal_stage'] = item.get('properties',{}).get('dealstage',{}).get('value','')
    info['deal_type'] = item.get('properties',{}).get('dealtype',{}).get('value','')
    info['amount'] = to_integer(item.get('properties',{}).get('amount',{}).get('value',''))
    info['amount_in_home_currency'] = to_integer(item.get('properties',{}).get('amount_in_home_currency',{}).get('value',''))
    info['closed_lost_reason'] = item.get('properties',{}).get('closed_lost_reason',{}).get('value','')
    info['closed_won_reason'] = item.get('properties',{}).get('closed_won_reason',{}).get('value','')
    info['forecast_close_date'] = to_date(item.get('properties',{}).get('forecast_close_date',{}).get('value',None)) # example of custom field
    info['close_date'] = to_date(item.get('properties',{}).get('closedate',{}).get('value',None))
    info['description'] = item.get('properties',{}).get('description',{}).get('value','')
    info['pipeline'] = item.get('properties',{}).get('pipeline',{}).get('value','')
    info['num_notes'] = to_integer(item.get('properties',{}).get('num_notes',{}).get('value',''))
    info['num_associated_contacts'] = to_integer(item.get('properties',{}).get('num_associated_contacts',{}).get('value',''))
    info['num_contacted_notes'] = to_integer(item.get('properties',{}).get('num_contacted_notes',{}).get('value',''))
    info['notes_last_contacted'] = to_date(item.get('properties',{}).get('notes_last_contacted',{}).get('value',None))
    info['notes_last_updated'] = to_date(item.get('properties',{}).get('notes_last_updated',{}).get('value',None))
    info['notes_next_activity_date'] = to_date(item.get('properties',{}).get('notes_next_activity_date',{}).get('value',None))
    info['created_at'] = to_date(item.get('properties',{}).get('createdate',{}).get('value',None))

    return info