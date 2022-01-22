# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *

class HouseLivingSpider(scrapy.Spider):
    name = "house_living"
    allowed_domains = ["thehouseliving.com"]
    start_urls = ['https://www.thehouseliving.com/ricerca-immobili.html?tipologia=Affitto%7C0&comune=&vani=0&mqda=&mqfinoa=&prezzo_a=0']
    country = 'Italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    thousand_separator='.'
    scale_separator=','
    pos = 1
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)
    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        property_urls = response.css('#properties .link-arrow::attr(href)').extract()
        for property_url in property_urls:
            yield Request(url=property_url,callback=self.populate_item)
        try:
            next_page = response.css('.active+ li a::attr(href)')[0].get()
            next_page = 'https://www.thehouseliving.com' + next_page
            yield Request(url=next_page,callback=self.parse)
        except:
            pass
    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css("h1::text")[0].extract()
        property_type = response.css("dd a::text")[0].extract()
        if "Appartamenti" in property_type:
            property_type = 'apartment'
        elif 'Villa' in property_type:
            property_type = 'house'
        else:
            return
        external_id = response.css(".property-title figure::text")[-1].extract()
        external_id = external_id.strip()[5:]
        description = response.css("#description p:nth-child(1)::text")[0].extract()
        utilities = None
        try:
            utilities = response.css("#description p::text").extract()
            utilities = ' '.join(utilities)
            if 'Spese condominiali' in utilities:
                utilities = int(utilities.split('€ all’anno (')[1].split('€ al mese).')[0])
            else:
                utilities = None
        except:
            pass
        square_meters = response.css("dd:nth-child(18)::text")[0].extract()
        square_meters = int(square_meters[:-2])
        rent = response.css("#quick-summary .price::text")[0].extract()
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return
        room_count = response.css("dd:nth-child(22)::text")[0].extract()
        room_count = int(room_count)
        bathroom_count = response.css("dd:nth-child(24)::text")[0].extract()
        bathroom_count = int(bathroom_count)
        deposit = None
        try:
            deposit = response.css("dd:nth-child(28)::text")[0].extract()
            if any(char.isdigit() for char in deposit):
                deposit = int(''.join(x for x in deposit if x.isdigit()))
        except:
            pass
        city = response.css("dd:nth-child(12)::text")[0].extract()
        region = response.css("dd:nth-child(10)::text")[0].extract()
        address = region + ', ' + city
        longitude,latitude = extract_location_from_address(address)
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)
        latitude = str(latitude)
        longitude = str(longitude)

        energy_label = None
        try:
            energy_label = response.css("#description img::attr(src)")[0].extract()
            energy_label = energy_label.split("assets/")[1].split(".png")[0]
        except:
            pass

        images = response.css(".image-popup::attr(href)").extract()

        features = response.css("#property-features li::text").extract()
        furnished = None
        if 'Arredato: Arredato' in features or 'Arredato: Parzialmente Arredato' in features:
            furnished = True
        elevator = None
        if 'Ascensore:  ' in features:
            elevator = True
        balcony = None
        if 'Balcone: Si' in features:
            balcony = True
        terrace = None
        if 'Terrazzo: Si\r\n\r\n m' in features:
            terrace = True
        floor = None
        try:
            floor = [x for x in features if "Piano:" in x][0].strip()
            if "PianoNobile" in floor or "Multipiano" in floor:
                floor = floor[7:19]
            else:
                floor = floor[7:9]
            floor = floor.strip()
        except:
            pass
        parking = None
        try:
            parking = [x for x in features if "Parcheggio:" in x][0]
            parking = True
        except:
            pass


        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.pos) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "House & Living Real Estate") # String
        item_loader.add_value("landlord_phone", "+39 0117538618") # String
        item_loader.add_value("landlord_email", "info@thehouseliving.com") # String

        self.pos += 1
        yield item_loader.load_item()