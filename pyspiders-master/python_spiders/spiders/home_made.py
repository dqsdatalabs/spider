# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re, json
from ..loaders import ListingLoader
from python_spiders.helper import string_found, format_date, remove_white_spaces
import dateparser
 
class HomeMadeSpider(scrapy.Spider):
    name = "home_made"
    allowed_domains = ["home-made"]
    start_urls = (
        'http://www.www.home-made.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    headers = {
        'authority': 'webapi.home-made.com',
        'accept': 'application/json, text/plain, */*',
        'sec-fetch-dest': 'empty',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36',
    }
    def start_requests(self):
        url = "https://webapi.home-made.com/properties"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        datas = json.loads(response.text)
        for data in datas['listings']:
            property_id = data['_id']
            link = "https://webapi.home-made.com/listing/{}".format(property_id)
            yield scrapy.Request(url=link, callback=self.get_property_details, headers=self.headers, dont_filter=True )

    def get_property_details(self, response):
        property_json = json.loads(response.text)
        property_type_j = property_json['propType']
        if property_type_j is not None:
            if 'house' in property_type_j.lower():
                property_type = 'house'
            elif 'studio' in property_type_j.lower():
                property_type = 'studio'
            elif 'flat' in property_type_j.lower() or 'apartment' in property_type_j.lower():
                property_type = 'apartment'
            
            else:
                property_type = ''
            external_id = property_json['_id'] 
            external_link = "https://www.home-made.com/property/{}".format(external_id)
            address = property_json['listingName']
            city = str(property_json['city'])
            title=property_json['listingName']
            zipcode = address.split(',')[-1].strip()
            rent = str(property_json['rent']) + '£'   
            images = []
            floor_plan = ''
            for img in property_json['media']['images']:
                if 'https://www.home-made.com/static/img/whatsapp.png' not in img: 
                    images.append(img['url'])
                if img['tags'] and 'floorplan' in img['tags'].lower():
                    floor_plan = img['url'] 
            available_date = property_json['dateAvailableString']
            parking = ''
            furnished = ''
            for feature in property_json['SpecialFeatures']:
                if 'parking' in feature.lower() or 'true' in str(property_json['Parking']).lower():
                    parking = True    
                if 'furnished' in feature.lower() or 'furnishing' in str(property_json['furnishing']).lower():
                    furnished = True
            descriptions = property_json['longdescription'].split('\r\n')
            utility = ''
            for dec in descriptions:
                if 'utility' in dec.lower():
                    try:
                        utility = re.findall(r'\d+', dec)[0]  
                    except:
                        utility = ''
            if property_type:
                item_loader = ListingLoader(response=response)
                item_loader.add_value('property_type', property_type)
                item_loader.add_value("title",title)
                item_loader.add_value('external_id', external_id)
                item_loader.add_value('external_link', external_link)
                item_loader.add_value('address', address)
                item_loader.add_value('city', city)
                item_loader.add_value('longitude', str(property_json['long']))
                item_loader.add_value('latitude', str(property_json['lat']))
                if zipcode.count(" ")>1: zipcode = zipcode.split(" ")[-1]
                item_loader.add_value('zipcode', zipcode)
                item_loader.add_value('description', property_json['longdescription'])
                item_loader.add_value('rent_string', rent)

                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
                item_loader.add_value('images', images)
                if parking: 
                    item_loader.add_value('parking', True)     
                if furnished:
                    item_loader.add_value('furnished', True)
                if 'PetFriendly' in str(property_json['PetFriendly']).lower():
                    item_loader.add_value('pets_allowed', True)
                if 'dishwasher' in property_json['longdescription'].lower():
                    item_loader.add_value('dishwasher', True)
                if 'balcony' in property_json['longdescription'].lower():
                    item_loader.add_value('balcony', True)
                if 'terrace' in property_json['longdescription'].lower():
                    item_loader.add_value('terrace', True) 
                if 'third floor' in property_json['longdescription'].lower():
                    item_loader.add_value('floor', '3')
                if utility:
                    item_loader.add_value('utilities', str(utility))
                if floor_plan: 
                    item_loader.add_value('floor_plan_images', floor_plan)
                if str(property_json['bedrooms']) != '0':
                    item_loader.add_value('room_count', str(property_json['bedrooms']))
                elif property_type == "studio":
                    item_loader.add_value('room_count', "1")
                if property_json['bathrooms']:
                    item_loader.add_value('bathroom_count', str(int(float(property_json['bathrooms']))))
                item_loader.add_value('landlord_name', 'Home Made')
                item_loader.add_value('landlord_email', ' info@home-made.com')
                item_loader.add_value('landlord_phone', '+44 207 846 0122')
                item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
                yield item_loader.load_item()
