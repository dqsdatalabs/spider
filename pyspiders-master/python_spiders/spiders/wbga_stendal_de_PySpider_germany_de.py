# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates

class wbga_stendal_de_PySpider_germanySpider(scrapy.Spider):
    name = "wbga_stendal_de"
    start_urls = ['https://www.wbga-stendal.de/wohnen/wohnungen/?index=0']
    allowed_domains = ["wbga-stendal.de"]
    page_number = 6
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
        urls = response.css('#wohnung-liste > div > a::attr(href)').extract()
        for i in range(len(urls)):
            urls[i] = 'https://www.wbga-stendal.de/wohnen/wohnungen/' + urls[i]
            room_count = int(response.css('#wohnung-liste > div:nth-child('+str(i+1)+') > div.wohnung-daten > div > div.wohnung-infos > div:nth-child(1) > div.wohnung-info-wert::text').get())
            square_meters = response.css('#wohnung-liste > div:nth-child('+str(i+1)+') > div.wohnung-daten > div > div.wohnung-infos > div:nth-child(2) > div.wohnung-info-wert::text').get()
            floor = response.css('#wohnung-liste > div:nth-child('+str(i+1)+') > div.wohnung-daten > div > div.wohnung-infos > div:nth-child(4) > div.wohnung-info-wert::text').get()
            rent = response.css('#wohnung-liste > div:nth-child('+str(i+1)+') > div.wohnung-daten > div > div.wohnung-infos > div:nth-child(3) > div.wohnung-info-wert::text').get()
            address = response.css('#wohnung-liste > div:nth-child('+str(i+1)+') > div.wohnung-daten > div > div.pb-2::text').get()
            yield Request(url= urls[i],
            callback=self.populate_item,
            meta={
                'room_count':room_count,
                'square_meters':square_meters,
                'floor':floor,
                'rent':rent,
                'address':address
            })
        next_page = ("https://www.wbga-stendal.de/wohnen/wohnungen/?index="+ str(wbga_stendal_de_PySpider_germanySpider.page_number))
        if wbga_stendal_de_PySpider_germanySpider.page_number <= 8:
            wbga_stendal_de_PySpider_germanySpider.page_number += 6
            yield response.follow(next_page, callback=self.parse)

    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        room_count = response.meta.get('room_count')
        address = response.meta.get('address')
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)        
        square_meters = response.meta.get('square_meters')
        square_meters = int(square_meters.split(',')[0])

        floor = response.meta.get('floor')
        
        rent = response.meta.get('rent')
        rent = int(rent.split(',')[0])

        title = response.css('body > main > div:nth-child(1) > div > h1::text').get()
        description = response.css('body > main > div:nth-child(2) > div > div:nth-child(4) > div.col-md-6.pb-5.pb-md-0 > div.wd-table-content *::text').extract()
        temp = ''
        for i in range(len(description)):
            temp = temp + ' ' + description[i]
        description = temp
        images = response.css('.w-100::attr(src)').extract()
        energy_label = response.css('body > main > div:nth-child(2) > div > div:nth-child(5) > div.col-md-6.pb-5.pb-md-0 > div.wd-table-content > div:nth-child(4) > div.wd-table-wert::text').get()
        heating_cost = response.css('body > main > div:nth-child(2) > div > div.row.pt-4.ml-0.mr-0 > div:nth-child(2) > div.wd-table-content > div:nth-child(3) > div.wd-table-wert::text').get()
        water_cost = response.css('body > main > div:nth-child(2) > div > div.row.pt-4.ml-0.mr-0 > div:nth-child(2) > div.wd-table-content > div:nth-child(4) > div.wd-table-wert::text').get()
        heating_cost = int(heating_cost.split(',')[0])
        water_cost = int(water_cost.split(',')[0])
        utilities = response.css('body > main > div:nth-child(2) > div > div.row.pt-4.ml-0.mr-0 > div:nth-child(2) > div.wd-table-content > div:nth-child(2) > div.wd-table-wert::text').get()
        utilities = int(utilities.split(',')[0])
        pets_allowed = None
        balcony = None
        amenities = response.css('body > main > div:nth-child(2) > div > div:nth-child(4) > div:nth-child(2) > div.wd-table-content > div::text').get().lower()
        if 'balkon' in amenities:
            balcony = True
        if 'haustiere' in amenities:
            pets_allowed = True
        

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
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
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "wbga-stendal") # String
        item_loader.add_value("landlord_phone", "(03931) 530 600") # String
        item_loader.add_value("landlord_email", "info@wbga-stendal.de") # String

        self.position += 1
        yield item_loader.load_item()
