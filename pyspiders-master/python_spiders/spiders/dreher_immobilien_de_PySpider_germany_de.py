# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import *


class dreher_immobilien_de_PySpider_germanySpider(scrapy.Spider):
    name = "dreher_immobilien_de"
    start_urls = ['https://www.dreher-immobilien.de/immobilienangebote/?mt=rent&category=14']
    allowed_domains = ["dreher-immobilien.de"]
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
        urls = response.css('#immo > div > div > div > p.h4.immo-listing__title.text-truncate > a::attr(href)').extract()
        for i in range(len(urls)):
            rent = response.css('#immo > div > div:nth-child('+str(i+1)+') > div > div:nth-child(3) > a::text').get()
            title = response.css('#immo > div > div:nth-child('+str(i+1)+') > div > p.h4.immo-listing__title.text-truncate > a::text').get()
            yield Request(url= urls[i],
            callback= self.populate_item,
            meta={
                'rent':rent,
                'title':title
            })
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get('title')
        rent = response.meta.get('rent')
        rent = rent.replace('€','')
        if '.' in rent:
            rent = rent.replace('.','')
        if ',' in rent:
            rent = rent.split(',')[0]
        rent = int(rent)

        city = None
        get_City = response.css('body > div.container.pt-4 > div > div.col-24.col-lg-24.pb-2 > h1::text').get()
        try:
            city = get_City.split('in')[1]
        except:
            pass


        deposit = response.css('body > div.immo-expose__list-price-container.pt-3 > div > div > div.col-24.col-lg-15 > div.immo-expose__list-price.pt-2.pb-3 > div.row.my-1 > div.col-12.col-md-16::text').get()
        deposit = deposit.replace('€','')
        if '.' in deposit:
            deposit = deposit.replace('.','')
        deposit = int(deposit)


        energy_label = None
        try:
            energy_label = response.css('body > div.epass.immo-expose__background.pb-3 > div > div > div.col-24.col-md-8.offset-md-1 > ul > li:nth-child(5) > span::text').get().strip()
            if len(energy_label) > 2:
                energy_label = None
        except:
            pass

        square_meters = response.css('body > div.container.pt-4 > div > div.col-24.col-lg-9.position-static > div > div:nth-child(3) > div > ul > li:nth-child(1) > span.value::text').get()
        if ',' in square_meters:
            square_meters = int(square_meters.split(',')[0])
        else:
            square_meters = int(square_meters.split('m')[0])
        room_count = response.css('body > div.container.pt-4 > div > div.col-24.col-lg-9.position-static > div > div:nth-child(3) > div > ul > li:nth-child(2) > span.value::text').get()
        if ',' in room_count:
            room_count = int(room_count.split(',')[0])+1
        else:
            room_count = int(room_count)
        images = response.css('#exGallery > a::attr(href)').extract()
        floor = response.css('body > div.immo-expose__list-price-container.pt-3 > div > div > div.col-24.col-lg-15 > div.row > div.col-24.col-md-16 > ul > li:nth-child(2) > span.value::text').get()
        bathroom_count = 1
        try:
            bathroom_count = response.css('body > div.immo-expose__list-price-container.pt-3 > div > div > div.col-24.col-lg-15 > div.row > div.col-24.col-md-16 > ul > li:nth-child(5) > span.key::text').get()
            if 'bad mit' in bathroom_count.lower():
                bathroom_count = 1
        except:
            pass
        if len(floor) > 1:
            floor = None
        list = response.css('.immo-expose__list-price--list span ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        external_id = None
        if 'Objekt-Nr ' in list:
            external_id = list.split('Objekt-Nr ')[1].split(' ')[0]
        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split('Nebenkosten ')[1].split(' ')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))

        description = response.css('script:contains("Beschreibung") ::text').get()
        description = remove_white_spaces(description)
        desc1 = description.split('"Beschreibung"> ')[1].split(' </b-tab>')[0]
        desc2 = description.split('"Ausstattung"> ')[1].split(' </b-tab>')[0]
        desc3 = description.split('"Lage"> ')[1].split(' </b-tab>')[0]
        desc4 = description.split('"Sonstiges"> ')[1].split(' </b-tab>')[0]
        description = desc1 + desc2 + desc3 + desc4
        description = description.replace('<p>', '')
        description = description.replace('</p>', '')
        description = description.replace('<br />', '')
        description = description.replace('<collapse-group text-open="mehr anzeigen" text-close="weniger anzeigen">', '')
        description = description.replace('</collapse-group>', '')
        description = description_cleaner(description)

        city = remove_white_spaces(city)

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        #item_loader.add_value("zipcode", zipcode) # String
        #item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

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
        item_loader.add_value("landlord_name", "dreher-immobilien") # String
        item_loader.add_value("landlord_phone", "0 60 32 92 91 60") # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
