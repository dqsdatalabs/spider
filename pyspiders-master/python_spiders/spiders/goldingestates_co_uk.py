# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib, json
from ..loaders import ListingLoader
from python_spiders.helper import  remove_white_spaces, extract_rent_currency, extract_number_only
import re

class GoldingestatesCoUkSpider(scrapy.Spider):
    name = "goldingestates_co_uk"
    allowed_domains = ["www.goldingestates.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    api_url = 'https://www.goldingestates.co.uk/wp-json/properties/v1/get-properties/'
    params = {'q': '',
              'tax': '',
              'beds_min': '0',
              'beds_max': '15',
              'price_min': '0',
              'price_max': '0',
              'order': 'DESC',
              'orderby': 'date'}
    position=0

    def start_requests(self):
        start_urls = [
                        {'tax':'1'},
                        {'tax':'2'}
                    ]
        
        for url in start_urls:
            params1 = copy.deepcopy(self.params)
            params1["tax"] = url["tax"]
            yield scrapy.Request(url=self.api_url + "?" + urllib.parse.urlencode(params1),
                                 callback=self.parse,
                                 meta={'request_url': self.api_url + "?" + urllib.parse.urlencode(params1),
                                       'params': params1})
            
    def parse(self, response, **kwargs):
        data = json.loads(response.body.decode("utf-8"))
        listings=data['body']['properties']
        for listing in listings:
            yield scrapy.Request(
                url=listing["url"],
                callback=self.get_property_details,
                meta={'request_url': listing["url"],
                      'name': listing['name'],
                      'zipcode': listing["postcode"],
                      'availability':listing['availability'],
                      'area': listing['area'],
                      'bedrooms': listing['bedrooms'],
                      'bathrooms': listing['bathrooms'],
                      'address': listing["location"]["address"],
                      'latitude': listing["location"]['lat'],
                      'longitude': listing["location"]['lng'],
                      'property_type': listing["property_type"]}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        if response.status == 404:
            return
        room_count= response.meta.get('bedrooms')
        zipcode = response.meta.get('zipcode')
        status = response.meta.get('availability')
        properties = response.meta.get('property_type')
        address = response.meta.get('address')
        area = response.meta.get('area')

        bathrooms = response.meta.get('bathrooms')
        property_type = response.xpath('.//*[@class="feature type type-apartment"]/*[@class="lower"]/text()').extract_first()
        bathroom_count = response.xpath('.//*[@class="feature data-bathrooms"]/*[@class="lower"]/text()').extract_first()
        rooms = response.xpath('.//*[@class="feature data-bedrooms"]/*[@class="lower"]/text()').extract_first()
        period = response.xpath('.//*[@class="feature data-price"]/*[@class="lower"]/text()').extract_first()
        features = response.xpath('.//*[@class="features-list"]//li/text()').extract()
        feature_str = ', '.join(features).lower()
        images = response.xpath('.//*[@class="image-container"]//*[@class="bg-image"]/@style').extract()
        apartment_types = ["appartement", "apartment", "flat","penthouse", "duplex", "triplex", "maisonette"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', 'villa',"terrace", "detach", "cottage"]
        studio_types = ["studio"]
        availability = response.xpath('.//*[@class="feature data-available"]/*[@class="lower"]/text()').extract_first()
        if status and "available" in status.lower() or availability and "available" in remove_white_spaces(availability).lower():
            item_loader.add_value('zipcode',zipcode)
            item_loader.add_value('title',response.meta.get('name'))
            item_loader.add_value('external_link',response.meta.get('request_url'))
            item_loader.add_value('latitude',response.meta.get('latitude'))
            item_loader.add_value('longitude',response.meta.get('longitude'))
            item_loader.add_value("external_source", "Goldingestates_PySpider_{}_{}".format(self.country, self.locale))
            item_loader.add_value('landlord_name','Golding Estates')
            item_loader.add_value('landlord_phone','0151 227 1199')
            item_loader.add_value('landlord_email','info@goldingestates.co.uk')
            description = "".join(response.xpath('//*[@id="overview"]//p//text()').getall())
            item_loader.add_value('description', description)
            item_loader.add_xpath('deposit','.//*[@class="feature data-deposit"]/*[@class="upper"]/text()')
            
            prop_type = ""
            if get_p_type_string(description):
                prop_type = get_p_type_string(description)
            
            if type(area)==list:
                area = ', '.join(area)
            if len(area)>0:
                address_complete = address+', '+area+', '+zipcode
                item_loader.add_value('address',address_complete)
            else:
                address_complete = address+', '+zipcode
                item_loader.add_value('address',address_complete)
            
            for i in images:
                image_url = re.search(r'(?<=url.)\S+',i)
                item_loader.add_value('images',image_url.group().strip(')'))
                
            city = response.xpath("//div[@class='the-location']/h2/text()").get()
            if city:
                if "City" in city:
                    item_loader.add_value("city", city.split(" ")[0])
                else:
                    item_loader.add_value("city", city)
            
            rent = response.xpath("//div[contains(@class,'data-price')]/span[@class='upper']/text()").get()
            if rent:
                if period and "week" in period.lower():
                    rent = int(float(rent.replace("£","").replace(",",".")))*4
                elif period:
                    rent = rent.replace("£","").replace(",",".")
                item_loader.add_value("rent", int(float(rent)))
            item_loader.add_value("currency", "GBP")
            
            if prop_type:
                item_loader.add_value("property_type", prop_type)
            
            elif property_type:
                if any (i in remove_white_spaces(property_type).lower() for i in studio_types):
                    item_loader.add_value('property_type', "studio")
                elif any (i in remove_white_spaces(property_type).lower() for i in house_types):
                    item_loader.add_value('property_type', "house")
                elif any (i in remove_white_spaces(property_type).lower() for i in apartment_types):
                    item_loader.add_value('property_type', 'apartment')
                    
            elif len(properties)>0:
                if any (i in remove_white_spaces(properties).lower() for i in studio_types):
                    item_loader.add_value('property_type', "studio")
                elif any (i in remove_white_spaces(properties).lower() for i in house_types):
                    item_loader.add_value('property_type', "house")
                elif any (i in remove_white_spaces(properties).lower() for i in apartment_types):
                    item_loader.add_value('property_type', 'apartment')
            else: return
            
            if rooms and extract_number_only(rooms) != '0':
                item_loader.add_value('room_count',extract_number_only(rooms))
            elif room_count != '0':
                item_loader.add_value('room_count',room_count)
                
            if bathroom_count and extract_number_only(bathroom_count) !='0':
                item_loader.add_value('bathroom_count',extract_number_only(bathroom_count))           
            elif bathrooms != '0':
                item_loader.add_value('bathroom_count',bathrooms)
                
            if re.search(r'un[^\w]*furnish',feature_str):
                item_loader.add_value('furnished',False)
            elif re.search(r'not[^\w]*furnish',feature_str):
                item_loader.add_value('furnished',False)
            elif re.search(r'furnish',feature_str):
                item_loader.add_value('furnished',True)
            if re.search(r'dish[^\w]*washer',feature_str):
                item_loader.add_value('dishwasher',True)
            if re.search(r'swimming[^\w]*pool',feature_str) or 'pool' in feature_str:
                item_loader.add_value('swimming_pool',True)
            if 'terrace' in feature_str or 'terrace' in properties.lower():
                item_loader.add_value('terrace',True)
            elif property_type and 'terrace' in property_type.lower():
                item_loader.add_value('terrace',True)
            if 'balcony' in feature_str:
                item_loader.add_value('balcony',True)
            if 'parking' in feature_str:
                item_loader.add_value('parking',True)
            if 'lift' in feature_str or 'elevator' in feature_str:
                item_loader.add_value('elevator',True)
            if re.search(r'no[^\w]*pet\w{0,1}[^\w]*allow',feature_str):
                item_loader.add_value('pets_allowed',False)
            elif re.search(r'pet\w{0,1}[^\w]*not[^\w]*allow',feature_str):
                item_loader.add_value('pets_allowed',False)
            elif re.search(r'pet\w{0,1}[^\w]*allow',feature_str):
                item_loader.add_value('pets_allowed',True)
                
            self.position += 1
            item_loader.add_value('position', self.position)
            yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "bedroom" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None