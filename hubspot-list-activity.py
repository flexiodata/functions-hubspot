
# ---
# name: hubspot-list-activity
# deployed: true
# config: index
# title: HubSpot Activity List
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
#   - name: activity_type
#     type: string
#     description: The activity type associated with the engagement
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
#     type: string
#     description: The id of the creator of the engagement
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
        page = content.get('results',[])

        for item in page:

            row = OrderedDict()
            row['engagement_id'] = str(item.get('engagement',{}).get('id',''))
            row['portal_id'] = str(item.get('engagement',{}).get('portalId',''))

            row['deal_id'] = ''
            ids = item.get('associations',{}).get('dealIds',[])
            if len(ids) > 0:
                row['deal_id'] = str(ids[0])

            row['company_id'] = ''
            ids = item.get('associations',{}).get('companyIds',[])
            if len(ids) > 0:
                row['company_id'] = str(ids[0])

            row['type'] = item.get('engagement',{}).get('type','').lower()
            row['activity_type'] = item.get('engagement',{}).get('activityType','')
            row['status'] = item.get('metadata',{}).get('status','')
            row['title'] = item.get('metadata',{}).get('title','')
            row['subject'] = item.get('metadata',{}).get('subject','')
            row['active'] = item.get('engagement',{}).get('active','')
            row['created_by'] = str(item.get('engagement',{}).get('createdBy',''))
            row['created_at'] = to_date(item.get('engagement',{}).get('createdAt',None))
            row['last_updated'] = to_date(item.get('engagement',{}).get('lastUpdated',None))
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
