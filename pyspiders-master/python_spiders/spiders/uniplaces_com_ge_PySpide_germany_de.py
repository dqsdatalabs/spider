# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates

class uniplaces_com_ca_PySpider_germanySpider(scrapy.Spider):
    name = "uniplaces_com_ge"
    start_urls = ['https://www.uniplaces.com/accommodation/berlin?page=1']
    allowed_domains = ["uniplaces.com"]
    country = 'Germany' 
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1




    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)


    
    def start_requests(self):
        counter = 1
        counter_1 = 1
        for i in range (0,8):
            yield Request(url= 'https://search-api-new.uniplaces.com/offers?page='+str(counter)+'&city=DE-berlin&ne=52.685897155833125,13.727485804392927&sw=52.35163937446247,13.111564783885115&limit=48&key=xgqlqs',
                        callback=self.parse,
                        body='',
                        method='GET')
            counter += 1
        for i in range (0,2):
            yield Request(url= 'https://search-api-new.uniplaces.com/offers?page='+str(counter_1)+'&city=DE-munich&ne=48.242942554048,11.979249423204237&sw=48.02690620603578,11.246834117985145&limit=48&key=w3cr8f',
                        callback=self.parse,
                        body='',
                        method='GET')
            counter_1 += 1
        yield Request(url='https://search-api-new.uniplaces.com/offers?city=DE-frankfurt&ne=50.19921807831675,9.215156733111598&sw=49.84629359886483,8.143989740924098&limit=48&key=n7ahcu',
                    callback=self.parse,
                    body='',
                    method='GET')
        yield Request(url='https://search-api-new.uniplaces.com/offers?city=DE-leipzig&ne=51.43289691729918,12.597207251811597&sw=51.26134550239122,12.171944772700044&limit=48&key=u9jcc2',
                    callback=self.parse,
                    body='',
                    method='GET')
        yield Request(url='https://search-api-new.uniplaces.com/offers?city=DE-hamburg&ne=53.63259399324758,10.179076087109365&sw=53.46941790589469,9.808287512890615&limit=48&key=hbvppi',
                    callback=self.parse,
                    body='',
                    method='GET')
        yield Request(url='https://search-api-new.uniplaces.com/offers?city=DE-stuttgart&ne=48.82261812549502,9.299431262047506&sw=48.76184279180004,9.09641977769229&limit=48&key=txldf8',
                    callback=self.parse,
                    body='',
                    method='GET')
        yield Request(url='https://search-api-new.uniplaces.com/offers?city=DE-dusseldorf&ne=51.270720568786906,6.9061498443847995&sw=51.184721455032125,6.640761355615268&limit=48&key=rs2gk6',
                    callback=self.parse,
                    body='',
                    method='GET')



    def parse(self, response, **kwargs):
        parsed_response = json.loads(response.body)
        for item in parsed_response['data']:
            
            latlng = item['attributes']['property']['coordinates']
            latitude = latlng[0]
            longitude = latlng[1]
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            url_id = item['id']
            url_id_hash = item['attributes']['accommodation_provider']['id']
            url = 'https://www.uniplaces.com/accommodation/'+city+'/'+url_id+'?search-id='+url_id_hash
            external_id = item['id']
            title = item['attributes']['accommodation_offer']['title']
            rent = item['attributes']['accommodation_offer']['price']['amount']
            tempo = item['attributes']['photos']
            hashed_image = []
            
            for i in range(len(tempo)):
                
                tempo[i] = tempo[i]['hash']
                hashed_image.append(tempo[i])
                hashed_image[i] = 'https://cdn-static-new.uniplaces.com/property-photos/' + hashed_image[i] + '/x-large.jpg'
            images = hashed_image
            room_count = item['attributes']['property']['number_of_rooms']
            bathroom_count = item['attributes']['property']['number_of_bathrooms']
            property_type = item['attributes']['property']['type']
            available_date = item['attributes']['accommodation_offer']['available_from']
            yield Request(url=url, callback=self.populate_item,
            meta={'external_id':external_id,
                  'title':title,
                  'property_type': property_type,
                  'address': address,
                  'city': city,
                  'zipcode': zipcode,
                  'rent':rent,
                  'images':images,
                  'room_count':room_count,
                  'bathroom_count':bathroom_count,
                  'latitude':str(latitude),
                  'longitude':str(longitude),
                  'available_date':available_date
                  })



    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        external_id = response.meta.get("external_id")
        title = response.meta.get("title")
        address = response.meta.get("address")
        city = response.meta.get("city")
        zipcode = response.meta.get("zipcode")
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        room_count = response.meta.get("room_count")
        bathroom_count = response.meta.get("bathroom_count")
        rent = response.meta.get("rent")/100
        images = response.meta.get("images")
        property_type = response.meta.get("property_type")
        available_date = response.meta.get("available_date")
        available_date = available_date.split('T')[0]
        
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        # item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        #item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent)) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "uniplaces") # String
        # item_loader.add_value("landlord_phone", landlord_number) # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
