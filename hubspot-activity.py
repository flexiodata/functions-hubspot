
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
#   - name: engagement_id
#     type: integer
#     description: The id for the engagement
#   - name: deal_id
#     type: string
#     description: The deal id for the engagement
#   - name: company_id
#     type: string
#     description: The company id for the engagement
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
        for item in data:
            item = get_item_info(item)
            buffer = buffer + json.dumps(item, default=to_string) + "\n"
        yield buffer

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

def get_item_info(item):

    info = OrderedDict()

    info['portal_id'] = item.get('engagement',{}).get('portalId','')
    info['engagement_id'] = item.get('engagement',{}).get('id')
    info['deal_id'] = ''
    ids = item.get('associations',{}).get('dealIds',[])
    if len(ids) > 0:
        info['deal_id'] = str(ids[0])
    info['company_id'] = ''
    ids = item.get('associations',{}).get('companyIds',[])
    if len(ids) > 0:
        info['company_id'] = str(ids[0])
    info['type'] = item.get('engagement',{}).get('type','').lower()
    info['activity_type'] = item.get('engagement',{}).get('activityType','')
    info['activity_date'] = to_date(item.get('engagement',{}).get('timestamp',None))
    info['status'] = item.get('metadata',{}).get('status','')
    info['title'] = item.get('metadata',{}).get('title','')
    info['subject'] = item.get('metadata',{}).get('subject','')
    info['active'] = item.get('engagement',{}).get('active','')
    info['created_by'] = item.get('engagement',{}).get('createdBy','')
    info['created_at'] = to_date(item.get('engagement',{}).get('createdAt',None))
    info['updated_at'] = to_date(item.get('engagement',{}).get('lastUpdated',None))

    return info
