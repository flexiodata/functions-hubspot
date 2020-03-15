
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

    # map this function's property names to the API's property names
    property_map = OrderedDict()
    property_map['first_name'] = 'firstname'
    property_map['last_name'] = 'lastname'
    property_map['email'] = 'email'
    property_map['phone'] = 'phone'
    property_map['phone_mobile'] = 'mobilephone'
    property_map['job_title'] = 'jobtitle'
    property_map['address'] = 'address'
    property_map['city'] = 'city'
    property_map['state'] = 'state'
    property_map['zip'] = 'zip'
    property_map['country'] = 'country'
    property_map['linkedin_bio'] = 'linkedinbio'
    property_map['created_date'] = 'createdate'
    property_map['modified_date'] = 'lastmodifieddate'

    # list of this function's properties we'd like to query
    properties = list(property_map.keys())

    # map the list of requested properties to hubspot properties; if none are
    # available, include a blank placeholder
    mapped_properties = [property_map.get(p,'') for p in properties]

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
        contacts = content.get('contacts',[])
        for item in contacts:
            row = [item.get('properties').get(p,{}).get('value','') or '' for p in properties]
            data.append(row)

        has_more = content.get('has-more', False)
        next_cursor_id = content.get('vid-offset')
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
