# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates

class quester_de_PySpider_germanySpider(scrapy.Spider):
    name = "quester_de"
    start_urls = ['https://www.quester.de/immobilienangebote/?mt=rent&category=14&radius=5']
    allowed_domains = ["quester.de"]
    country = 'Germany'
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1
    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def parse(self, response, **kwargs):
        urls = response.css('#immobilien > div > div > div > div:nth-child(3) > a::attr(href)').extract()
        for i in range(len(urls)):
            rent = response.css('#immobilien > div > div:nth-child('+str(i+1)+') > div > div:nth-child(3) > a::text').get()
            if '.' in rent:
                rent = rent.replace('.','')
            rent = rent.replace('€','')
            if ',' in rent:
                rent = rent.split(',')[0]
            rent = int(rent)
            address = response.css('#immobilien > div > div:nth-child('+str(i+1)+') > div > p.text-center.text-small.text-truncate.immo-listing__subtitle::text').get()
            address = address.replace('Wohnung zur Miete in','')
            address = address.strip()
            yield Request(url= urls[i],
            callback= self.populate_item,
            meta={
                'rent':rent,
                'address':address
            })
       
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta.get('rent')
        address = response.meta.get('address')
        city = address.split(' ')[1]
        zipcode = None
        latitude = None
        longitude = None
        try:
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        except:
            pass
        
        floor = response.css('body > div.immo-expose__list-price-container.pt-3 > div > div > div.col-24.col-lg-15 > div.row > div.col-24.col-md-16 > ul > li:nth-child(2) > span.value::text').get()
        external_id = response.css('body > div.immo-expose__list-price-container.pt-3 > div > div > div.col-24.col-lg-15 > div.row > div.col-24.col-md-16 > ul > li:nth-child(1) > span.value::text').get()

        deposit = None
        try:
            deposit = response.css('body > div.immo-expose__list-price-container.pt-3 > div > div > div.col-24.col-lg-15 > div.immo-expose__list-price.pt-2.pb-3 > div.row.my-1 > div.col-12.col-md-16::text').get()
            deposit = deposit.replace('€','')
            if '.' in deposit:
                deposit = deposit.replace('.','')
            deposit = int(deposit)
        except:
            pass
        utilities = response.css('body > div.immo-expose__list-price-container.pt-3 > div > div > div.col-24.col-lg-15 > div.immo-expose__list-price.pt-2.pb-3 > div:nth-child(3) > div.col-24.col-md-16 > ul > li:nth-child(2) > span.value::text').get()
        utilities = utilities.replace('€','')
        utilities = int(utilities)

        images= response.css('#exGallery > a::attr(href)').extract()   
        energy_label = None
        try:
            energy_label = response.css('body > div.epass.immo-expose__background.pb-3 > div > div > div.col-24.col-md-8.offset-md-1 > ul > li:nth-child(5) > span::text').get()
        except:
            pass
        
        room_count = response.css('body > div.container.pt-lg-7.pt-3 > div > div.col-24.col-lg-9.position-static > div > div:nth-child(3) > div > ul > li:nth-child(2) > span.value::text').get()
        room_count = int(room_count)
        square_meters = response.css('body > div.container.pt-lg-7.pt-3 > div > div.col-24.col-lg-9.position-static > div > div:nth-child(3) > div > ul > li:nth-child(1) > span.value::text').get()
        if ',' in square_meters:
            square_meters = int(square_meters.split(',')[0])
        else:
            square_meters = int(square_meters.split('m')[0])
        
        title = response.css('small::text').get()
        
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        #item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
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
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Armin Quester Immobilien GmbH") # String
        item_loader.add_value("landlord_phone", "+49 203 282870") # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
