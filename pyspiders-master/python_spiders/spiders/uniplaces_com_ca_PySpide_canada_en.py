# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates

class uniplaces_com_ca_PySpider_canadaSpider(scrapy.Spider):
    name = "uniplaces_com_ca"
    start_urls = ['https://www.uniplaces.com/accommodation/toronto?page=1']
    allowed_domains = ["uniplaces.com"]
    country = 'Canada' 
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1




    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)


    
    def start_requests(self):
        counter = 1
        for i in range (0,2):
            yield Request(url= 'https://search-api-new.uniplaces.com/offers?page='+str(counter)+'&city=CA-toronto&ne=44.963086810148525,-78.59483911886375&sw=43.52651673205611,-83.86513814586037&limit=48&key=zh1hn1',
                        callback=self.parse,
                        body='',
                        method='GET')
            counter += 1
        yield Request(url='https://search-api-new.uniplaces.com/offers?city=CA-montreal&ne=45.59785897740739,-73.41327574487303&sw=45.4053542696978,-73.71986296411131&limit=48&key=ngmxzw',
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
        if room_count == 0:
            room_count = 1

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
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "uniplaces") # String
        # item_loader.add_value("landlord_phone", landlord_number) # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
