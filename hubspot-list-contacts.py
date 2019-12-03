
# ---
# name: hubspot-list-contacts
# deployed: true
# title: HubSpot Contacts List
# description: Returns a list of contacts from HubSpot.
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#     required: false
# examples:
#   - ' '
#   - '"*"'
#   - '"first_name, last_name, phone, email"'
# notes: |
#   The following properties are available:
#     * `first_name`: first name of the contact
#     * `last_name`: last name of the contact
#     * `email`: email address of the contact
#     * `phone`: phone number of the contact
#     * `phone_mobile`: mobile phone number of the contact
#     * `job_title`: job title of the contact
#     * `address`: address of the contact
#     * `city`: city in which the contact is located
#     * `state`: state in which the contact is located
#     * `zip`: zip code in which the contact is located
#     * `country`: country in which the contact is located
#     * `linkedin_bio`: biography of the contact on LinkedIn
#     * `created_date`: date the record for this contact was created
#     * `modified_date`: last date the record for this contact was modified
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
    properties = [p.lower().strip() for p in input['properties']]

    # if we have a wildcard, get all the properties
    if len(properties) == 1 and properties[0] == '*':
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

    try:

        # make the request
        url_query_params = {
            'hapikey': auth_token,
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
        response = requests.get(url)
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
