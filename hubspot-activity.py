
# ---
# name: hubspot-activity
# deployed: true
# config: index
# title: HubSpot Activity
# description: Returns a list of activity from HubSpot.
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
#     type: integer
#     description: The portal id for the engagement
#   - name: owner_id
#     type: integer
#     description: The id of the owner of the engagement
#   - name: owner_first_name
#     type: string
#     description: The first name of the owner of the engagement
#   - name: owner_last_name
#     type: string
#     description: The last name of the owner of the engagement
#   - name: engagement_id
#     type: integer
#     description: The id for the engagement
#   - name: deal_id
#     type: integer
#     description: The deal id for the engagement
#   - name: company_ids
#     type: string
#     description: A delimited list of company ids associated with the engagement
#   - name: type
#     type: string
#     description: The type of the engagement
#   - name: activity_type
#     type: string
#     description: The activity type associated with the engagement
#   - name: activity_date
#     type: string
#     description: The date of the engagement
#   - name: status
#     type: string
#     description: The status of a particular activity for the engagement
#   - name: title
#     type: string
#     description: The title of a particular activity for the engagement
#   - name: subject
#     type: string
#     description: The subject of a particular activity for the engagement
#   - name: active
#     type: string
#     description: The status of the engagement; true if the engagement is active and false otherwise
#   - name: created_by
#     type: integer
#     description: The id of the creator of the engagement
#   - name: created_at
#     type: string
#     description: The creation date of the engagement
#   - name: updated_at
#     type: string
#     description: The date the engagement was last updated
# examples:
#   - ' '
#   - '"*"'
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
    for data in get_data(flex.vars):
        flex.output.write(data)

def get_data(params):

    # get the api key from the variable input
    auth_token = dict(params).get('hubspot_connection',{}).get('access_token')

    # see here for more info:
    # https://developers.hubspot.com/docs/methods/engagements/get-all-engagements
    # https://developers.hubspot.com/docs/methods/engagements/engagements-overview
    # note: pagination mechanism different from other api calls; compare deal pagination

    # see here to get owner information:
    # https://developers.hubspot.com/docs/methods/owners/owners_overview
    # https://developers.hubspot.com/docs/methods/owners/get_owners

    # STEP 1: get the owner info
    headers = {
        'Authorization': 'Bearer ' + auth_token,
    }
    url = 'https://api.hubapi.com/owners/v2/owners'
    url_query_params = {'includeInactive': True}
    url_query_str = urllib.parse.urlencode(url_query_params)

    page_url = url + '?' + url_query_str
    response = requests_retry_session().get(page_url, headers=headers)
    response.raise_for_status()
    content = response.json()
    data = content

    owners = {}
    for item in data:
        owners[item.get('ownerId')] = item

    # STEP 3: get the engagement info
    headers = {
        'Authorization': 'Bearer ' + auth_token,
    }
    url = 'https://api.hubapi.com/engagements/v1/engagements/paged'

    page_size = 250
    page_cursor_id = None
    while True:

        url_query_params = {'limit': page_size}
        if page_cursor_id is not None:
            url_query_params['offset'] = page_cursor_id
        url_query_str = urllib.parse.urlencode(url_query_params)

        page_url = url + '?' + url_query_str
        response = requests_retry_session().get(page_url, headers=headers)
        response.raise_for_status()
        content = response.json()
        data = content.get('results',[])

        if len(data) == 0: # sanity check in case there's an issue with cursor
            break

        buffer = ''
        for header_item in data:
            deal_items = header_item.get('associations',{}).get('dealIds')
            if deal_items is None or len(deal_items) == 0:
                deal_items = [None] # if no deals, use empty deal so we return activity information
            for deal_id in deal_items:
                detail_item = {'deal_id': deal_id}
                item = get_item_info(header_item, detail_item, owners)
                buffer = buffer + json.dumps(item, default=to_string) + "\n"
        yield buffer

        has_more = content.get('hasMore', False)
        if has_more is False:
            break

        page_cursor_id = content.get('offset')

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(429, 500, 502, 503, 504),
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
    return datetime.utcfromtimestamp(int(ts)/1000).strftime('%Y-%m-%dT%H:%M:%S')

def to_integer(value):
    try:
        return int(value)
    except:
        return value

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def get_item_info(header_item, detail_item, owners):

    info = OrderedDict()

    info['portal_id'] = to_integer(header_item.get('engagement',{}).get('portalId'))

    owner_id = to_integer(header_item.get('engagement',{}).get('ownerId'))
    info['owner_id'] = owner_id
    info['owner_first_name'] = owners.get(owner_id,{}).get('firstName')
    info['owner_last_name'] = owners.get(owner_id,{}).get('lastName')

    info['engagement_id'] = to_integer(header_item.get('engagement',{}).get('id'))
    info['deal_id'] = to_integer(detail_item.get('deal_id',None))

    company_ids = header_item.get('associations',{}).get('companyIds',[])
    info['company_ids'] = ', '.join([str(i) for i in company_ids]) # convert to comma-delimited string

    info['type'] = header_item.get('engagement',{}).get('type','').lower()
    info['activity_type'] = header_item.get('engagement',{}).get('activityType','')
    info['activity_date'] = to_date(header_item.get('engagement',{}).get('timestamp',None))
    info['status'] = header_item.get('metadata',{}).get('status','')
    info['title'] = header_item.get('metadata',{}).get('title','')
    info['subject'] = header_item.get('metadata',{}).get('subject','')
    info['active'] = header_item.get('engagement',{}).get('active','')
    info['created_by'] = to_integer(header_item.get('engagement',{}).get('createdBy'))
    info['created_at'] = to_date(header_item.get('engagement',{}).get('createdAt',None))
    info['updated_at'] = to_date(header_item.get('engagement',{}).get('lastUpdated',None))

    return info
