
# ---
# name: hubspot-list-contacts
# deployed: true
# config: index
# title: HubSpot Contacts List
# description: Returns a list of contacts from HubSpot.
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
#   - name: first_name
#     type: string
#     description: The first name of the contact
#   - name: last_name
#     type: string
#     description: The last name of the contact
#   - name: email
#     type: string
#     description: The email address of the contact
#   - name: phone
#     type: string
#     description: The phone number of the contact
#   - name: phone_mobile
#     type: string
#     description: The mobile phone number of the contact
#   - name: job_title
#     type: string
#     description: The job title of the contact
#   - name: address
#     type: string
#     description: The address of the contact
#   - name: city
#     type: string
#     description: The city in which the contact is located
#   - name: state
#     type: string
#     description: The state in which the contact is located
#   - name: zip
#     type: string
#     description: The zip code in which the contact is located
#   - name: country
#     type: string
#     description: The country in which the contact is located
#   - name: linkedin_bio
#     type: string
#     description: The biography of the contact on LinkedIn
#   - name: created_date
#     type: string
#     description: The date the record for this contact was created
#   - name: modified_date
#     type: string
#     description: The last date the record for this contact was modified
# examples:
#   - ' '
#   - '"*"'
#   - '"first_name, last_name, phone, email"'
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
    # https://developers.hubspot.com/docs/methods/contacts/get_contacts
    # note: pagination mechanism different from other api calls; compare activity/deal pagination

    try:

        # make the request
        headers = {
            'Authorization': 'Bearer ' + auth_token,
        }
        url_query_params = {
            'count': 100,
            'property': ''
        }
        if cursor_id is not None:
            url_query_params['vidOffset'] = cursor_id

        request_properties = [
            'firstname','lastname','email','phone','mobilephone','jobtitle', 'address',
            'city','state','zip','country','linkedinbio','createdate', 'lastmodifieddate'
        ]

        url_query_str = urllib.parse.urlencode(url_query_params)
        url_query_str += "&property=" + "&property=".join(request_properties)
        url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all?' + url_query_str

        # get the response
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        content = response.json()

        # get the data and the next cursor
        data = []
        page = content.get('contacts',[])

        for item in page:
            row = OrderedDict()
            row['first_name'] = item.get('properties').get('firstname',{}).get('value','')
            row['last_name'] = item.get('properties').get('lastname',{}).get('value','')
            row['email'] = item.get('properties').get('email',{}).get('value','')
            row['phone'] = item.get('properties').get('phone',{}).get('value','')
            row['phone_mobile'] = item.get('properties').get('mobilephone',{}).get('value','')
            row['job_title'] = item.get('properties').get('jobtitle',{}).get('value','')
            row['address'] = item.get('properties').get('address',{}).get('value','')
            row['city'] = item.get('properties').get('city',{}).get('value','')
            row['state'] = item.get('properties').get('state',{}).get('value','')
            row['zip'] = item.get('properties').get('zip',{}).get('value','')
            row['country'] = item.get('properties').get('country',{}).get('value','')
            row['linkedin_bio'] = item.get('properties').get('linkedinbio',{}).get('value','')
            row['created_date'] = to_date(item.get('properties').get('createdate',{}).get('value',''))
            row['modified_date'] = to_date(item.get('properties').get('lastmodifieddate',{}).get('value',''))
            data.append(row)

        has_more = content.get('has-more', False)
        next_cursor_id = content.get('vid-offset')
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
