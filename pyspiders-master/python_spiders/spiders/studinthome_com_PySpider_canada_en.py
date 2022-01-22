# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates

class studinthome_com_PySpider_canadaSpider(scrapy.Spider):
    name = "studinthome_com"
    start_urls = ['https://studinthome.com/search-results/?city=&listing_type=apartment']
    allowed_domains = ["studinthome.com"]
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
        yield Request(url='https://studinthome.com/wp-admin/admin-ajax.php/?action=homey_half_map&arrive=&depart=&guest=&keyword=&pets=&bedrooms=&rooms=&room_size=&search_country=&search_city=&search_area=&listing_type=apartment&min-price=&max-price=&country=&state=&city=&area=&booking_type=&search_lat=&search_lng=&radius=&start_hour=&end_hour=&amenity=&facility=&layout=grid&num_posts=179&sort_by=a_price&paged=0&security=6e237175bc',
                    callback=self.parse,
                    body='',
                    method='POST')


    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response['listings']:
            url = item['url']
            external_id = item['id']
            title = item['title']
            latitude = item['lat']
            longitude = item['long']
            room_count = item['bedrooms']
            bathroom_count = item['baths']
            rent = item['price']
            yield Request(url=url, callback=self.populate_item,
            meta={'external_id':external_id,
                  'title':title,
                  'latitude':latitude,
                  'longitude':longitude,
                  'room_count':room_count,
                  'bathroom_count':bathroom_count,
                  'rent':rent
                  })

    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        external_id = str(response.meta.get("external_id"))
        title = response.meta.get("title")
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        room_count = int(response.meta.get("room_count"))
        bathroom_count = int(response.meta.get("bathroom_count"))
        rent = response.meta.get("rent")
        
        
        property_type = 'apartment'
        if room_count == 0:
            property_type = 'studio'
            room_count = 1

        rent = rent.split('CAD</sup>')[1].split('.00')[0]
        if ',' in rent:
            rent = int(rent.replace(',',''))
        else:
            rent = int(rent)
       
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)


        description = response.css('#about-section > div.block > div > p *::text').extract()
        tempo = ''
        for i in range(len(description)):
            tempo = tempo + ' ' + description[i]
        description = tempo
        tempo = tempo.lower()

        images = response.css('.fanboxTopGallery-item::attr(src)').extract()
        landlord_name = response.css('#host-section > div > div.block-head > div > div.media-body > h2 > span::text').get()

        pets_allowed = response.css('#rules-section > div > div > div > div.block-right > ul > li:nth-child(2) > strong::text').get()
        if pets_allowed == "Yes":
            pets_allowed = True
        else:
            pets_allowed = False

        washing_machine = None
        if 'washer' in tempo:
            washing_machine = True
        
        balcony = None
        if 'balcony' in tempo or 'balconies' in tempo:
            balcony = True
        
        swimming_pool = None
        if 'pool' in tempo:
            swimming_pool = True
        
        terrace = None
        if 'terrasse' in tempo:
            terrace = True
        
        furnished = None
        if 'unfurnished' in tempo:
            furnished = False
        elif 'furnished' in tempo:
            furnished = True
        
        pets_allowed = None
        if 'pets are not allowed' in tempo:
            pets_allowed = False
        
        parking = None
        if 'parking' in tempo:
            parking = True

            

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

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

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", "+1 (844)-538-2525") # String
        item_loader.add_value("landlord_email", "hello@studinthome.com") # String

        self.position += 1
        yield item_loader.load_item()
