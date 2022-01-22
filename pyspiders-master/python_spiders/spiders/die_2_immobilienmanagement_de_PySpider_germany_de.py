# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates
from ..helper import *

class die_2_immobilienmanagement_de_PySpider_germanySpider(scrapy.Spider):
    name = "die_2_immobilienmanagement_de"
    start_urls = ['https://die-2-immobilienmanagement.de/ff/immobilien/?schema=houses_rent&price=&ffpage=1&sort=date','https://die-2-immobilienmanagement.de/ff/immobilien/?schema=flat_rent&price=&ffpage=1&sort=date']
    allowed_domains = ["die-2-immobilienmanagement.de"]
    country = 'Germany'
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1
    page_number = 2
    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def parse(self, response, **kwargs):
        urls = response.css('#ff-default > div.FFestateview-default-overview-list > article > a::attr(href)').extract()
        for i in range(len(urls)):
            rent = response.css('#ff-default > div.FFestateview-default-overview-list > article:nth-child('+str(i+1)+') > a > div > div.FFestateview-default-overview-estate-price.ff-color-primary > div > span::text').get()
            rent = rent.split(',')[0]
            if '.' in rent:
                rent = rent.replace('.','')
            rent = int(rent)
            external_id = response.css('#ff-default > div.FFestateview-default-overview-list > article:nth-child('+str(i+1)+') > a > div > div.FFestateview-default-overview-estate-details > div.identifier > div > span:nth-child(2)::text').get()
            room_count = response.css('#ff-default > div.FFestateview-default-overview-list > article:nth-child('+str(i+1)+') > a > div > div.FFestateview-default-overview-estate-details > div.rooms > div > span:nth-child(2)::text').get()
            if '.5' in room_count:
                room_count = int(room_count.split('.5')[0]) + 1
            else:
                room_count = int(room_count)
            square_meters = int(response.css('#ff-default > div.FFestateview-default-overview-list > article:nth-child('+str(i+1)+') > a > div > div.FFestateview-default-overview-estate-details > div.livingarea > div > span:nth-child(2)::text').get())
            property_type = response.css('#ff-default > div.FFestateview-default-overview-list > article:nth-child('+str(i+1)+') > a > div > div.FFestateview-default-overview-estate-type::text').get()
            if 'Doppelhaushälfte' in property_type:
                property_type = 'house'
            else:
                property_type = 'apartment'
            yield Request(url= urls[i],
            callback= self.populate_item,
            meta={
                'rent':rent,
                'external_id':external_id,
                'room_count':room_count,
                'square_meters':square_meters,
                'property_type':property_type
            }
            )
        next_page = ("https://die-2-immobilienmanagement.de/ff/immobilien/?schema=flat_rent&price=&ffpage="+ str(die_2_immobilienmanagement_de_PySpider_germanySpider.page_number) + "&sort=date")
        if die_2_immobilienmanagement_de_PySpider_germanySpider.page_number <= 6:
            die_2_immobilienmanagement_de_PySpider_germanySpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)

    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.css('h1.entry-title::text').get()
        description = response.css('#ff-default > div:nth-child(3) > div > p::text').extract()
        temm = ''
        for i in range(len(description)):
            temm = temm + ' ' + description[i]
        description = temm.lower()

        rent = response.meta.get('rent')
        utilities = None
        try:
            utilities = response.css('#ff-default > div.FFestateview-default-details-content.FFestateview-default-details-content-blank > div > div > div.FFestateview-default-details-content-details > ul > li:nth-child(4) > span:nth-child(2)::text').get()
            utilities = int(utilities.split(',')[0])
        except:
            pass
            
        if utilities is None:
            utilities = response.css('#ff-default > div.FFestateview-default-details-content.FFestateview-default-details-content-blank > div > div > div.FFestateview-default-details-content-details > ul > li:nth-child(5) > span:nth-child(2)::text').get()
            utilities = int(utilities.split(',')[0])
        try:
            if '.' in utilities:
                utilities = None
        except:
            pass
        external_id = response.meta.get('external_id')
        property_type = response.meta.get('property_type')
        room_count = response.meta.get('room_count')
        square_meters = response.meta.get('square_meters')
        landlord_name = 'die-2-immobilienmanagement'
        landlord_phone = '+49(212) 38374183'
        try:
            landlord_name = response.css('#ff-default > div.FFestateview-default-details-content.FFestateview-default-details-content-blank > div > div > div:nth-child(2) > div > div > div:nth-child(1) > div.FFestateview-default-details-agent-name > span::text').get()
            landlord_phone = response.css('#ff-default > div.FFestateview-default-details-content.FFestateview-default-details-content-blank > div > div > div:nth-child(2) > div > div > div:nth-child(1) > div.FFestateview-default-details-agent-details > div > a:nth-child(2)::text').get()
        except:
            pass
        

        
        images = response.css('.FFestateview-default-details-main-image a::attr(href)').extract()
        floor_plan_images = response.css('#ff-default > div.FFestateview-default-groundplot > div > div > a::attr(href)').extract()
        
        amenities = response.css('.ja::text').extract()
        temp = ''
        for i in range(len(amenities)):
            temp = temp + ' ' + amenities[i]
        amenities = temp.lower()

        balcony = None
        elevator = None
        washing_machine = None
        if 'balcon' in amenities:
            balcony = True
        if 'aufzug' in amenities:
            elevator = True
        if 'waschmaschine' in description:
            washing_machine = True

        floor = response.css('#ff-default > div.FFestateview-default-details-content.FFestateview-default-details-content-blank > div > div > div.FFestateview-default-details-content-details > ul > li:nth-child(8) > span:nth-child(2)::text').get()
        if len(floor) > 2: 
            floor = None
        info = response.css('#ff-default > div.FFestateview-default-details-content.FFestateview-default-details-content-blank > div > div > div.FFestateview-default-details-content-details > ul > li > span *::text').extract()
        tempo = ''
        for i in range(len(info)):
            tempo = tempo + ' ' + info[i]
        info = tempo.lower()
        bathroom_count = 1
        if 'bades' in info:
            bathroom_count = 1

        list = response.css('li ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)
        utilities = None
        if 'Nebenkosten ' in list:
            utilities = list.split('Nebenkosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        address = None
        if 'Lage ' in list:
            address = list.split('Lage ')[1].split(' Miete zzgl. NK')[0]

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, bebo = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        parking = None
        if 'stellplatz' in description.lower() or 'stellplatz' in list.lower():
            parking = True
        if 'waschmasch' in description.lower() or 'waschmasch' in list.lower():
            washing_machine = True
        dishwasher = None
        if 'geschirr' in description.lower() or 'geschirr' in list.lower():
            dishwasher = True
        terrace = None
        if 'terras' in description.lower() or 'terras' in list.lower():
            terrace = True
        if 'aufzug' in description.lower() or 'aufzug' in list.lower():
            elevator = True
        if 'balkon' in description.lower() or 'balkon' in list.lower():
            balcony = True
        furnished = None
        if 'renoviert' in description.lower() or 'renoviert' in list.lower():
            furnished = True

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
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", "info@die-2-immobilienmanagement.de") # String

        self.position += 1
        yield item_loader.load_item()
