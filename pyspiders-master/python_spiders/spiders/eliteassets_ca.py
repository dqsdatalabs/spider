import json
import re

import scrapy
from scrapy import Request

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader


class EliteassetsCaSpider(scrapy.Spider):
    name = 'eliteassets_ca'
    allowed_domains = ['eliteassets.ca']
    start_urls = ['https://eliteassets.ca/search-listings',
                  ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    token = ''

    def start_requests(self):
        header = {
            "Cookie": '_gcl_au=1.1.1982649305.1637086496; _ga=GA1.2.1118388531.1637086497; _gid=GA1.2.393244112.1637086497; connect.sid=s%3AnYM-NBZyZm5UFjy4WZH5wXNcbla3lTiS.GJJNfE9pwP9gZMQBLC2oOeuJIzvo0RbEDtXRbyfmHOc; %5Bunique%5D.*=true; %5Bsession%5D.*=true; %5Bunique%5D.%2Fhome=true; %5Bsession%5D.%2Fhome=true; %5Bunique%5D.%2Fsearch-listings=true; %5Bsession%5D.%2Fsearch-listings=true; _gat_wsbx=1',
            "Accept-Language": 'en-US,en;q=0.9,ar-EG;q=0.8,ar;q=0.7',
            "Accept-Encoding": 'gzip, deflate, br'}
        yield Request(callback=self.parse_2,
                      url=self.start_urls[0],
                      headers=header)

    def parse_2(self, response):
        payload = {"mls_id":2000040998,
                   "sort":"ListPrice:descendant",
                   "size":20,
                   "from":0,
                   "query":"",
                   "StandardStatus":"Active,Active Under Contract",
                   "ListPrice_max":10000000000,
                   "ListPrice_min":0,
                   "BedroomsTotal_max":50,
                   "BedroomsTotal_min":0,
                   "BathroomsTotalInteger_max":50,
                   "BathroomsTotalInteger_min":0,
                   "LivingArea_max":100000,
                   "LivingArea_min":0,
                   "team_id":40998}
        script = response.css("script:contains('initiate_globals')").get()
        self.token = (re.findall('token":"([\w.-]+)"', script))[0]
        headers = {
            'content-type': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
            'Cookie' : '_gcl_au=1.1.1982649305.1637086496; _ga=GA1.2.1118388531.1637086497; _gid=GA1.2.393244112.1637086497; connect.sid=s%3AnYM-NBZyZm5UFjy4WZH5wXNcbla3lTiS.GJJNfE9pwP9gZMQBLC2oOeuJIzvo0RbEDtXRbyfmHOc; %5Bunique%5D.*=true; %5Bsession%5D.*=true; %5Bunique%5D.%2Fhome=true; %5Bsession%5D.%2Fhome=true; %5Bunique%5D.%2Fsearch-listings=true; %5Bsession%5D.%2Fsearch-listings=true; _gat_wsbx=1',
            'X-Auth-Token' : self.token
        }
        yield Request(url='https://eliteassets.ca/api/team/40998/mls/2000040998/listings/search',
                      callback=self.parse_3,
                      method='POST',
                      headers=headers,
                      body=json.dumps(payload))



    def parse_3(self, response, requests=None):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            external_link = response.url
            external_source = self.external_source

            id_array=[]
            if item['listing_key'] not in id_array:
                if 'ListPrice' in item['value'].keys():
                    id_array.append(item['listing_key'])
                    item_loader = ListingLoader(response=response)
                    external_link = 'https://eliteassets.ca/listing/montreal/773-outremont/2000040998:'+item['id']
                    external_id = item['listing_key']
                    external_source = self.external_source
                    title = None
                    description = item['value']['PublicRemarks']
                    city = item['value']['City']
                    zipcode = None
                    address =[]
                    address_info = item['value']['normalized_address']['parsed']
                    for info in address_info:
                        if info['label'] == 'postcode':
                            zipcode = info['value']
                        address.append(info['value'])

                    latitude = str(item['value']['Latitude'])
                    longitude = str(item['value']['Longitude'])
                    if item['value'].get('PropertySubType'):
                        property_type = item['value']['PropertySubType']
                        if property_type == 'condominium':
                            property_type = 'apartment'
                        elif property_type == 'townhouse':
                            property_type = "house"
                        else:
                            property_type ='apartment'
                    square_meters = sq_feet_to_meters(int(item['value']['LivingArea']))

                    if item['value'].get('RoomsTotal'):
                        room_count = item['value']['RoomsTotal']
                    else:
                        room_count = 1

                    if item['value'].get('BedroomsTotal'):
                        bathroom_count = item['value']['BedroomsTotal']
                    else:
                        bathroom_count = 1
                    available_date = None
                    images_arr = []
                    if item['value'].get('Photos'):
                        images = item['value']['Photos']
                        for i in images:
                            images_arr.append(i['url'])
                    images = images_arr
                    floor_plan_images = None
                    external_images_count = len(images)
                    rent = int((item['value']['ListPrice'])[:-2])
                    currency = "CAD"
                    deposit = None
                    prepaid_rent = None
                    utilities = None
                    water_cost = None
                    heating_cost = None
                    energy_label = None
                    if 'PetsAllowed' in item['value'].keys():
                        if item['value']['PetsAllowed'].get('No'):
                            pets_allowed = item['value']['PetsAllowed']['No']
                            if pets_allowed == 'true':
                                pets_allowed = True
                            else:
                                pets_allowed = False
                        else:
                            pets_allowed = False

                    if item['value'].get('Furnished'):
                        if item['value']['Furnished']:
                            furnished = True
                        else:
                            furnished = False
                    else:
                        furnished = False

                    floor = None

                    if item['value'].get('ParkingTotal'):
                        if item['value']['ParkingTotal'] > 0:
                            parking = True
                        else:
                            parking = False
                    else:
                        parking = False

                    elevator = None

                    balcony = None
                    if 'balcony' in description.lower():
                        balcony = True
                    else:
                        balcony = False

                    if 'terrace' in description.lower():
                        terrace = True
                    else:
                        terrace = False

                    swimming_pool = None
                    if 'pool' in description.lower():
                        swimming_pool = True
                    else:
                        swimming_pool = False


                    if item['value'].get('Appliances'):
                        if item['value']['Appliances'].get(' Washer'):
                            washing_machine = item['value']['Appliances'][' Washer']
                            if washing_machine == 'true':
                                washing_machine = True
                            else:
                                washing_machine = False
                        else:
                            washing_machine = False


                        if item['value']['Appliances'].get(' Dishwasher'):
                            dishwasher = item['value']['Appliances'][' Dishwasher']
                            if dishwasher == 'true':
                                dishwasher = True
                            else:
                                dishwasher = False
                        else:
                            dishwasher = False
                    else:
                        dishwasher = False
                        washing_machine = False

                    if washing_machine == False:
                        if (' washer' in description.lower()) or ('laundry' in description.lower()) or ('washing machine' in description.lower()):
                            washing_machine = True

                    if dishwasher == False:
                        if 'dishwasher' in description.lower():
                            dishwasher = True

                    landlord_name = 'Elite Assets'
                    landlord_email = 'office@eliteassets.ca'
                    landlord_phone = '514-612-0736'

            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('external_source', external_source)
            item_loader.add_value('title', title)
            item_loader.add_value('description', description)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('address', address)
            item_loader.add_value("latitude", str(latitude))
            item_loader.add_value("longitude", str(longitude))
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', int(int(square_meters)*10.764))
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathroom_count)
            item_loader.add_value('available_date', available_date)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("deposit", deposit)
            item_loader.add_value("prepaid_rent", prepaid_rent)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("water_cost", water_cost)
            item_loader.add_value("heating_cost", heating_cost)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)

            yield item_loader.load_item()
