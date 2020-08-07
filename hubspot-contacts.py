
# ---
# name: hubspot-contacts
# deployed: true
# config: index
# title: HubSpot Contacts
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
#   - name: portal_id
#     type: integer
#     description: The portal id for the contact
#   - name: vid
#     type: integer
#     description: The id for the contact
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
#   - name: created_at
#     type: string
#     description: The date the record for this contact was created
#   - name: updated_at
#     type: string
#     description: The last date the record for this contact was modified
# examples:
#   - ' '
#   - '"*"'
#   - '"first_name, last_name, phone, email"'
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
    # https://developers.hubspot.com/docs/methods/contacts/get_contacts
    # note: pagination mechanism different from other api calls; compare activity/deal pagination

    headers = {
        'Authorization': 'Bearer ' + auth_token,
    }
    url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all'

    request_properties = [
        'firstname','lastname','email','phone','mobilephone','jobtitle', 'address',
        'city','state','zip','country','linkedinbio','createdate', 'lastmodifieddate'
    ]

    page_size = 100
    page_cursor_id = None
    while True:

        url_query_params = {'count': page_size}
        if page_cursor_id is not None:
            url_query_params['vidOffset'] = page_cursor_id
        url_query_str = urllib.parse.urlencode(url_query_params)
        url_request_properties = "&property=" + "&property=".join(request_properties)

        page_url = url + '?' + url_query_str + url_request_properties
        response = requests_retry_session().get(page_url, headers=headers)
        response.raise_for_status()
        content = response.json()
        data = content.get('contacts',[])

        if len(data) == 0: # sanity check in case there's an issue with cursor
            break

        buffer = ''
        for item in data:
            item = get_item_info(item)
            buffer = buffer + json.dumps(item, default=to_string) + "\n"
        yield buffer

        has_more = content.get('has-more', False)
        if has_more is False:
            break

        page_cursor_id = content.get('vid-offset')

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

def get_item_info(item):

    info = OrderedDict()

    info['portal_id'] = item.get('portal-id')
    info['vid'] = item.get('vid')
    info['first_name'] = item.get('properties').get('firstname',{}).get('value','')
    info['last_name'] = item.get('properties').get('lastname',{}).get('value','')
    info['email'] = item.get('properties').get('email',{}).get('value','')
    info['phone'] = item.get('properties').get('phone',{}).get('value','')
    info['phone_mobile'] = item.get('properties').get('mobilephone',{}).get('value','')
    info['job_title'] = item.get('properties').get('jobtitle',{}).get('value','')
    info['address'] = item.get('properties').get('address',{}).get('value','')
    info['city'] = item.get('properties').get('city',{}).get('value','')
    info['state'] = item.get('properties').get('state',{}).get('value','')
    info['zip'] = item.get('properties').get('zip',{}).get('value','')
    info['country'] = item.get('properties').get('country',{}).get('value','')
    info['linkedin_bio'] = item.get('properties').get('linkedinbio',{}).get('value','')
    info['created_at'] = to_date(item.get('properties').get('createdate',{}).get('value',''))
    info['updated_at'] = to_date(item.get('properties').get('lastmodifieddate',{}).get('value',''))

    return info