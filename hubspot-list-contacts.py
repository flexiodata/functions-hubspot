
# ---
# name: hubspot-list-contacts
# deployed: true
# title: HubSpot Contacts List
# description: Returns a list of contacts from HubSpot
# params:
#   - name: properties
#     type: array
#     description: The properties to return (defaults to 'email'). See "Notes" for a listing of the available properties.
#     required: false
# examples:
# notes: |-
#   The following properties are allowed:
#     * `first_name`: first name of the person
#     * `last_name`: last name of the person
#     * `email`: email address of the person (default)
#     * `phone`: phone number of the person
#     * `phone_mobile`: mobile phone number of the person
#     * `job_title`: job title of the person
#     * `address`: address of the person
#     * `city`: city in which the person is located
#     * `state`: state in which the person is located
#     * `zip`: zip code in which the person is located
#     * `country`: country in which the person is located
#     * `linkedin`: biography of the person on LinkedIn
#     * `created_date`: date the record for this person was created
#     * `modified_date`: last date the record for this person was modified
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
    auth_token = dict(flex.vars).get('hubspot_api_key')
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
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': 'full_name'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    # if the input is valid return an error
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    try:

        # map this function's property names to the API's property names
        property_map = {
            'full_name': 'firstname',
            'last_name': 'lastname',
            'email': 'email',
            'phone': 'phone',
            'phone_mobile': 'mobilephone',
            'job_title': 'jobtitle',
            'address': 'address',
            'city': 'city',
            'state': 'state',
            'zip': 'zip',
            'country': 'country',
            'linkedin': 'linkedinbio',
            'created_date': 'createdate',
            'modified_date': 'lastmodifieddate'
        }

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # list of the HubSpot properties we'd like to query
        hubspot_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://developers.hubspot.com/docs/methods/contacts/get_contacts
        url_query_params = {
            'hapikey': auth_token,
            'count': 100,
            'property': ''
        }
        url_query_str = urllib.parse.urlencode(url_query_params)
        properties_str = "&property=".join(hubspot_properties)
        url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all?' + url_query_str + properties_str

        # get the response data as a JSON object
        response = requests.get(url)
        content = response.json()

        # return the info
        result = []
        result.append(properties)

        contacts = content.get('contacts',[])
        for contact in contacts:
            row = []
            for p in hubspot_properties:
                row.append(contact.get('properties').get(p,{}).get('value',''))
            result.append(row)

        # return the results
        result = json.dumps(result, default=to_string)
        flex.output.content_type = "application/json"
        flex.output.write(result)

    except:
        raise RuntimeError

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
