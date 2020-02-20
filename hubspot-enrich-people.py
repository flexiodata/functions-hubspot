
# ---
# name: hubspot-enrich-people
# deployed: true
# title: HubSpot People Enrichment
# description: Returns profile information of a contact in HubSpot based on email address.
# params:
#   - name: email
#     type: string
#     description: The email address of the person you wish you find.
#     required: true
#   - name: properties
#     type: array
#     description: The properties to return (defaults to `twitter_handle`). See "Returns" for a listing of the available properties.
#     required: false
# returns:
#   - name: first_name
#     type: string
#     description: The first name of the person
#   - name: last_name
#     type: string
#     description: The last name of the person
#   - name: email
#     type: string
#     description: The email address of the person
#   - name: job_title
#     type: string
#     description: The job title of the person
#   - name: city
#     type: string
#     description: The city in which the person is located
#   - name: state
#     type: string
#     description: The state in which the person is located
#   - name: company
#     type: string
#     description: The name of the company the person is associated with
#   - name: website
#     type: string
#     description: The website of the person
#   - name: twitter_handle
#     type: string
#     description: The person's Twitter handle (default)
#   - name: twitter_profile_photo_url
#     type: string
#     description: The URL of the person's Twitter profile photo
#   - name: conversion_event_cnt
#     type: string
#     description: The number of conversion events the person has completed
#   - name: lifecycle_stage
#     type: string
#     description: The lifecycle stage
#   - name: created_date
#     type: string
#     description: The date the record for this person was created
#   - name: modified_date
#     type: string
#     description: The last date the record for this person was modified
# examples:
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
    params['email'] = {'required': True, 'type': 'string'}
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': 'twitter_handle'}
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
    property_map['job_title'] = 'jobtitle'
    property_map['city'] = 'city'
    property_map['state'] = 'state'
    property_map['company'] = 'company'
    property_map['website'] = 'website'
    property_map['twitter_handle'] = 'twitterhandle'
    property_map['twitter_profile_photo_url'] = 'twitterprofilephoto'
    property_map['conversion_event_cnt'] = 'num_conversion_events'
    property_map['lifecycle_stage'] = 'lifecyclestage'
    property_map['created_date'] = 'createdate'
    property_map['modified_date'] = 'lastmodifieddate'

    try:

        # list of this function's properties we'd like to query
        properties = [p.lower().strip() for p in input['properties']]

        # if we have a wildcard, get all the properties
        if len(properties) == 1 and properties[0] == '*':
            properties = list(property_map.keys())

        # list of the HubSpot properties we'd like to query
        hubspot_properties = [property_map[p] for p in properties]

        # see here for more info:
        # https://developers.hubspot.com/docs/methods/contacts/get_contact_by_email
        headers = {
            'Authorization': 'Bearer ' + auth_token,
        }
        url_query_params = {
            'property': ''
        }
        url_email_str = urllib.parse.quote(input['email'])
        url_query_str = urllib.parse.urlencode(url_query_params)
        properties_str = "&property=".join(hubspot_properties)
        url = 'https://api.hubapi.com/contacts/v1/contact/email/' + url_email_str + '/profile?' + url_query_str + properties_str

        # get the response data as a JSON object
        response = requests.get(url, headers=headers)
        content = response.json()

        # limit the results to the requested properties
        result = []
        for p in hubspot_properties:
            result.append(content.get('properties').get(p,{}).get('value','') or '')
        result = [result]

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
