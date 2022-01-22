# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
import json
from datetime import datetime
import re



class BricksandmortargroupCoUkSpider(scrapy.Spider):
    
    name = "bricksandmortargroup_co_uk"
    allowed_domains = ["bricksandmortar.co.uk"]
    start_urls = ['https://www.bricksandmortargroup.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    
    def start_requests(self):
        
        start_urls = [{'api_url' : "https://api.bricksandmortargroup.co.uk/api/2.0/PropertySearch/GetPropertySearch/",
        'params' : {
            "pageNo":1,
            "pageSize":8,
            "searchField":"",
            "searchTerm":"",
            "orderID":1,
            "departmentID":0,
            "departmentName":"residential",
            "categoryName":"lettings",
            "priceFrequencyId":0,
            "priceMin":0,
            "priceMax":0,
            "bedroomsMin":0,
            "location":"",
            "searchRadius":3218,
            "mapLongitude":-1.60398,
            "mapLatitude":54.974248,
            "mapZoom":15,
            "mapRadius":500,
            "noMap":False,
            "includeLetOrSold":False,
            "propertyTypes":[],
            "searchGeos":[],
            "coordinates":[]
        }},
        {'api_url' : "https://api.bricksandmortargroup.co.uk/api/2.0/PropertySearch/GetPropertySearch/",
        'params' : {
            "pageNo":1,
            "pageSize":8,
            "searchField":"",
            "searchTerm":"",
            "orderID":1,
            "departmentID":0,
            "departmentName":"residential",
            "categoryName":"students",
            "priceFrequencyId":0,
            "priceMin":0,
            "priceMax":0,
            "bedroomsMin":0,
            "location":"",
            "searchRadius":3218,
            "mapLongitude":-1.60398,
            "mapLatitude":54.974248,
            "mapZoom":15,
            "mapRadius":500,
            "noMap":False,
            "includeLetOrSold":False,
            "propertyTypes":[],
            "searchGeos":[],
            "coordinates":[]
            }
        }]

        for url in start_urls:
            request_body = json.dumps(url.get('params'))
            yield scrapy.Request(url.get('api_url'),
                            callback= self.parse,
                            dont_filter=True,
                            method="POST",
                            body=request_body,
                            headers={'Content-Type': 'application/json; charset=UTF-8'},
                            meta = {'params' : url.get('params'), 'api_url' : url.get('api_url') } )
        
    def parse(self, response, **kwargs):
        params = response.meta.get('params')
        api_url = response.meta.get('api_url')
        temp_json = json.loads(response.body)
        num_pages = temp_json["paginationItems"][-2]['pageNumber']
   
        for i in range(1, num_pages+1):
            if i == 1:
                params["pageNo"] = i
            else:
                params["pageNo"] = str(i)
            request_body = json.dumps(params)
            yield scrapy.Request(api_url,
                         callback= self.get_property_details,
                         dont_filter=True,
                         method="POST",
                         body=request_body,
                         headers={'Content-Type': 'application/json; charset=UTF-8'},
                         meta = {'params' : params, 'api_url' : api_url } )

    def get_property_details(self, response):
         
        temp_json = json.loads(response.body)
        properties = temp_json['resultItems']
        for property_item in properties:
            item_loader = ListingLoader(response=response)
            external_id = property_item['referenceNumber']
            external_link = "https://www.bricksandmortargroup.co.uk/find-a-property/property-information/" + external_id
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('room_count', property_item['residential']['propertyBedrooms'])
            item_loader.add_value('bathroom_count', property_item['residential']['propertyBathrooms'])
            item_loader.add_value('images', ["https://cdn.bricksandmortargroup.co.uk/" + str(image['source']) for image in property_item['propertyAssets']])
            rent_type = property_item['residentialLetting']['rentFrequencyName']
            if rent_type == 'PW':
                item_loader.add_value('rent_string', '£' + str(property_item['residentialLetting']['rent'] * 4))
            else:
                item_loader.add_value('rent_string', '£' + str(property_item['residentialLetting']['rent']))
            
            property_branch = property_item['branchName']
            property_type = property_item['residential']['propertyTypeName']            
            title = str(item_loader.get_output_value('room_count')) + ' Bedroom ' + property_item['residential']['propertyStyleName'] + ' to Rent in ' + property_item['propertyAddress']['display']
            item_loader.add_value('title', title)
            item_loader.add_value('city', 'Newcastle Upon Tyne')
            item_loader.add_value('zipcode', property_item['propertyAddress']['postcode'])                        
            item_loader.add_value('address',property_item['propertyAddress']['street'] + ', ' + property_item['propertyAddress']['lineTwo'] + ', ' + item_loader.get_output_value('city') + ', ' + item_loader.get_output_value('zipcode'))
            item_loader.add_value('description', property_item['summary'])
            features = " ".join([x['description'] for x in property_item['features']])
            if property_item['latitude'] != 0 and property_item['longitude'] != 0:       
                item_loader.add_value('latitude', str(property_item['latitude']))
                item_loader.add_value('longitude', str(property_item['longitude']))

            check = re.search(r'(\d{2}(?:\w{2})? \w+ \d{4})', features.lower(), re.IGNORECASE)
            if check:
                if "august" in check.group(1):
                    available_date = check.group(1)
                    available_date = available_date.lower().replace("rd", "").replace("nd", "") .replace("th","")
                    available_date = datetime.strptime(available_date, "%d %B %Y").strftime('%Y-%m-%d')
                    item_loader.add_value('available_date', available_date)
                else:
                    available_date = check.group(1)
                    available_date = available_date.lower().replace("rd", "").replace("nd", "") .replace("th","").replace("st","")
                    available_date = datetime.strptime(available_date, "%d %B %Y").strftime('%Y-%m-%d')
                    item_loader.add_value('available_date', available_date)
            
            apartment_types = ["apartment", "flat", "penthouse", "duplex", "dakappartement", "triplex"]
            house_types = ['bungalow', 'maison', 'house', 'home',]
            studio_types = ["studio"]
            
            #property_type
            
            if any (i in property_type.lower() for i in studio_types):
                item_loader.add_value('property_type', 'studio')
            elif any (i in property_type.lower() for i in apartment_types) and property_branch == 'Student':
                item_loader.add_value('property_type', 'student_apartment')
            elif any (i in property_type.lower() for i in apartment_types) and property_branch != 'Student':
                item_loader.add_value('property_type', 'apartment')
            elif any (i in property_type.lower() for i in house_types):
                item_loader.add_value('property_type', 'house')
            else:
                return


            # https://www.bricksandmortargroup.co.uk/find-a-property/property-information/11526/
            if "parking" in features.lower():
                item_loader.add_value('parking', True)
            
            if "terrace" in features.lower():
                item_loader.add_value('terrace', True)

            if "swimming pool" in features.lower():
                item_loader.add_value('swimming_pool', True)
            
            if "elevator" in features.lower() or "lift" in features.lower():
                item_loader.add_value('elevator', True)
            
            if "balcony" in features.lower():
                item_loader.add_value('balcony', True)
            
            # https://www.bricksandmortargroup.co.uk/find-a-property/property-information/17FP2942/
            if " furnished" in features.lower():
                item_loader.add_value('furnished', True)

            if "dishwasher" in features.lower():
                item_loader.add_value('dishwasher', True)

            if "washing machine" in features.lower():
                item_loader.add_value('washing_machine', True)
            
            item_loader.add_value('landlord_phone', '0191 230 5577')
            item_loader.add_value('landlord_email', 'hello@bricksandmortargroup.co.uk')
            item_loader.add_value('landlord_name', 'Bricks & Mortar Group')

            self.position += 1
            item_loader.add_value('position', self.position)
            item_loader.add_value("external_source", "Bricksandmortargroup_PySpider_{}_{}".format(self.country, self.locale))

            yield item_loader.load_item()
