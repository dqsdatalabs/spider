# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address

class wpsq_ca_PySpider_canadaSpider(scrapy.Spider):
    name = "wpsq_ca"
    start_urls = ['https://wpsq.ca/']
    allowed_domains = ["wpsq.ca"]
    country = 'Canada' 
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    


    def parse(self, response, **kwargs):
        urls = response.css('.elementor-widget-button:nth-child(7) .elementor-size-sm::attr(href)').extract()
        for i in range(len(urls)):
            yield Request(url = urls[i],
            callback=self.populate_item)



    def populate_item(self, response):
    
        suites = response.css('body > div.elementor > div > section.elementor-section > div > div > div > div > div > div > div > ul > li').extract()
        counter = 2
        anchor = 1
        for i in range(len(suites)-1):
            item_loader = ListingLoader(response=response)
            
            room_count = response.css('body > div.elementor > div > section.elementor-section > div > div > div > div > div > div > div > ul > li:nth-child('+str(counter)+') > div.col.col-1::text').get()
            rent = response.css('body > div.elementor > div > section.elementor-section > div > div > div > div > div > div > div > ul > li:nth-child('+str(counter)+') > div.col.col-3::text').get()
            floor_plan_images = response.css('body > div.elementor > div > section.elementor-section > div > div > div > div > div > div > div > ul > li:nth-child('+str(counter)+') > div.col.col-4 > a::attr(href)').get()
            images = response.css('body > div.elementor > div > section.elementor-section > div > div > div > div.elementor-element> div > div > a:nth-child('+str(counter-1)+')::attr(href)').extract()
            
            counter += 1

            rent = rent.replace('$','')
            if ',' in rent:
                rent = int(rent.replace(',',''))
            else:
                rent = int(rent)

            property_type = 'apartment'
            
            if 'Studios' in room_count:
                room_count = 1
                property_type = 'studio'
            elif 'Junior' in room_count:
                room_count = int(room_count.split('Junior')[1].split('Bedroom')[0])
            else:
                room_count = int(room_count.split(' ')[0])
            
            title = response.css('body > div.elementor > div > section.elementor-section > div > div > div > div > div > h2::text').get()
            address = response.css('.elementor-element-015ca44 p *::text').extract()
            city = address[1].split(',')[0]
            zipcode = address[1].split(',')[1]
            temp = ''
            for i in range(len(address)):
                temp = temp + ' ' + address[i]
            address = temp

            longitude, latitude = extract_location_from_address(address)
            item_loader.add_value("external_link", response.url+f"#{anchor}") # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            #item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            #item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

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
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
            item_loader.add_value("landlord_name", "WELLESLEY PARLIAMENT SQUARE") # String
            item_loader.add_value("landlord_phone", "416-925-0000") # String
            item_loader.add_value("landlord_email", "PARLIAMENT650@WPSQ.CA") # String
            anchor +=1
            self.position += 1
            yield item_loader.load_item()
