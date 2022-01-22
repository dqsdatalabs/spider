# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates, extract_location_from_address

class grandcityproperty_de_PySpider_germanySpider(scrapy.Spider):
    name = "grandcityproperty_de"
    start_urls = ['https://www.grandcityproperty.de/wohnungssuche']
    allowed_domains = ["grandcityproperty.de"]
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
        urls = response.css('#result > div.content.row > div > div > div > div.info-button.row > a::attr(href)').extract()
        for i in range(len(urls)):
            urls[i] = 'https://www.grandcityproperty.de' + urls[i]
            rent = response.css('#result > div.content.row > div:nth-child('+str(i+1)+') > div > div > div.info-bottom.row > div.additionals > div:nth-child(1)::text').get()
            if '.' in rent:
                rent = rent.replace('.','')
            rent = int(rent.split(' ')[0])
            square_meters = response.css('#result > div.content.row > div:nth-child('+str(i+1)+') > div > div > div.info-bottom.row > div.additionals > div:nth-child(2)::text').get()
            square_meters = int(square_meters.split(' ')[0])
            room_count = None
            try:
                room_count = response.css('#result > div.content.row > div:nth-child('+str(i+1)+') > div > div > div.info-bottom.row > div.additionals > div:nth-child(3)::text').get()
                if ',5' in room_count:
                    room_count = int(room_count.split(',5')[0])+1
                else:
                    room_count = int(room_count.split(',')[0])
            except:
                pass
            if room_count is not None:
                yield Request(url= urls[i],
                callback= self.populate_item,
                meta={
                    'rent':rent,
                    'square_meters':square_meters,
                    'room_count':room_count
                }
                )

    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(8) > div > div > div.col-lg-8.col-md-12.col-sm-12.no-padding > div > h1 *::text').extract()
        temp = ''
        for i in range(len(title)):
            temp = temp + ' ' + title[i].strip()
        title = temp.strip()
        title = title.replace('  ','')

        room_count = response.meta.get('room_count')
        rent = response.meta.get('rent')
        square_meters = response.meta.get('square_meters')
        address = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > div.container > div > div.col-md-7.col-xs-10 > div > ul > li.active > span:nth-child(2)::text').get()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        floor = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(8) > div > div > div.col-lg-8.col-md-12.col-sm-12.no-padding > div > div.additionals > div:nth-child(3)::text').get()

        images = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > div.featured_slick_gallery > div.real-estate-detail-slide > div > img::attr(src)').extract()
        for i in range(len(images)):
            images[i] = 'https://www.grandcityproperty.de' + images[i]
        external_id = None
        try:
            external_id = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(9) > div > div > div.col-lg-8.col-md-12.col-sm-12.no-padding > div > div > ul > li:nth-child(1) > div > div.col-xs-6.text-right::text').get()
            external_id = external_id.strip()
        except:
            pass
        utlities = None
        try:
            utilities = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(9) > div > div > div.col-lg-4.col-md-12.col-sm-12.no-padding > div > div > ul > li:nth-child(2) > div > div.col-xs-5.text-right::text').get()
            utilities = int(utilities.split(',')[0])
        except:
            pass
        heating_cost = None
        try:
            heating_cost = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(9) > div > div > div.col-lg-4.col-md-12.col-sm-12.no-padding > div > div > ul > li:nth-child(3) > div > div.col-xs-5.text-right::text').get()
            heating_cost = int(heating_cost.split(',')[0])
        except:
            pass
        deposit = None
        try:
            deposit = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(9) > div > div > div.col-lg-4.col-md-12.col-sm-12.no-padding > div > div > ul > li:nth-child(6) > div > div.col-xs-5.text-right::text').get()
            if '.' in deposit:
                deposit = deposit.replace('.','')
            deposit = int(deposit.split(',')[0])
        except:
            pass
        available_date = None
        try:
            available_date = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(8) > div > div > div.col-lg-8.col-md-12.col-sm-12.no-padding > div > div.infos > div:nth-child(3) > strong::text').get()
            if 'Sofort' in available_date:
                available_date = None
        except:
            pass
        description = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(10) > div > div > div > div > p *::text').extract()
        tempo = ''
        for i in range(len(description)):
            tempo = tempo + ' ' + description[i]
        description = tempo

        additions = response.css('#realEstate > div.real-estate-mm-wrapper > div.content > section:nth-child(8) > div > div > div.col-lg-8.col-md-12.col-sm-12.no-padding > div > div.additionals').get()
        balcony = None
        elevator = None
        try:
            if 'Aufzug' in additions:
                elevator = True
            if 'Balkon' in additions:
                balcony = True
        except:
            pass

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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
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
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "grandcityproperty") # String
        item_loader.add_value("landlord_phone", "0151-14575531") # String
        item_loader.add_value("landlord_email", "post@grandcityproperty.de") # String

        self.position += 1
        yield item_loader.load_item()
