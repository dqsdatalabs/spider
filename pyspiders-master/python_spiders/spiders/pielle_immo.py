# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *


class PielleImmoSpider(scrapy.Spider):
    name = "pielle_immo"
    start_urls = ['https://www.pielleimmobiliare.it/status-immobile/affitto/']
    country = 'Italy' # Fill in the Country's name
    locale = 'it' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        property_urls = response.css('h4 a::attr(href)').extract()
        titles = response.css('h4 a::text').extract()
        for index,property_url in enumerate(property_urls):
            yield Request(url=property_url, callback=self.populate_item, meta={'title': titles[index]})
        try:
            next_page = response.css('.current+ .real-btn::attr(href)')[0].get()
            yield Request(url=next_page, callback=self.parse)
        except:
            pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta['title']
        if 'Capannone' in title or 'CAPANNONE' in title or 'Ufficio' in title or 'Negozio' in title:
            return
        property_type = 'apartment'
        external_id = response.css(".property-meta span:nth-child(1)::text")[0].extract()
        description = response.css("#overview p::text")[0].extract()
        rent = response.css(".price-and-type ::text")[7].extract()
        if any(char.isdigit() for char in rent):
            rent = ''.join(x for x in rent if x.isdigit())
            rent = int(rent[:-2])
        else:
            return
        parking = None
        if 'posti auto' in description.lower():
            parking = True
        latlng = response.css('script:contains("propertyMapData")::text').get()
        coords = latlng.split('lat":"')[1].split('","thumb":')[0]
        coords = coords.split('","lng":"')
        latitude = coords[0]
        longitude = coords[1]
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)
        square_meters = response.css(".property-meta span:nth-child(2)::text")[0].extract()
        square_meters = int(square_meters[:-2])
        room_count = response.css("span:nth-child(3)::text")[0].extract()
        room_count = int(room_count.strip()[0])
        bathroom_count = None
        try:
            bathroom_count = response.css("span:nth-child(4)::text")[0].extract()
            bathroom_count = int(bathroom_count.strip()[0])
        except:
            pass

        images = response.css(".slides a::attr(href)").extract()



        # # MetaData
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
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
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
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Pielle Immobiliare") # String
        item_loader.add_value("landlord_phone", "+39 035216151") # String
        item_loader.add_value("landlord_email", "infotre@pielleimmobiliare.it, pielleimmobiliare@pec.it") # String

        self.position += 1
        yield item_loader.load_item()
