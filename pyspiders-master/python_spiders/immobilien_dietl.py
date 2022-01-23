import scrapy
from gevent.libev.corecext import callback

from ..loaders import ListingLoader
from ..helper import *
import re
import json

from scrapy.loader import ItemLoader
from ..items import ListingItem


class ImmobilienDietlSpider(scrapy.Spider):
    name = 'immobilien_dietl'
    # allowed_domains = ['https://www.immobilien-dietl.de/mieten']
    start_urls = ['https://www.makler24.com/immobilien-vermarktungsart/miete/']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        apartments_urls=response.css(".xh-highlight a::attr(href)").getall()

        for apartment_url in apartments_urls:
            yield scrapy.Request(url="https://www.makler24.com/immobilien/"+apartment_url,callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):


        item_loader = ListingLoader(response=response)

        # Your scraping code goes here
        # Dont push any prints or comments in the code section
        # if you want to use an item from the item loader uncomment it
        # else leave it commented
        # Finally make sure NOT to use this format
        #    if x:
        #       item_loader.add_value("furnished", furnished)
        # Use this:
        #   balcony = None
        #   if "balcony" in description:
        #       balcony = True

        # # Enforces rent between 0 and 40,000 please dont delete these lines
        description=""
        room_count = None
        bathroom_count = None
        floor = None
        parking = True
        elevator = True
        balcony = True
        washing_machine = True
        dishwasher = True
        utilities = None
        terrace = True
        furnished = True
        property_type = None
        energy_label = None
        deposit = None
        available = None
        pets_allowed = True
        square_meters = None
        swimming_pool = 1
        external_id = None
        rent = None
        title=response.css(".property-title::text").get()
        description= response.css(".xh-highlight::text").get()
        city= response.css(".list-group-item:nth-child(6) .col-sm-7::text").get()
        rent = response.css('.list-group-item:nth-child(2) .col-sm-7::text').get()
        rent = int(extract_number_only(rent, '.', ','))
        landlord_number = '0 49 41 - 97 57 - 0'
        landlord_email = 'info@makler24.com'
        landlord_name = 'Harms & Harms'
        currency = "EUR"
        address = response.css(".list-group-item:nth-child(6) .col-sm-7::text").get()
        floor = response.css(".list-group-item:nth-child(9) .col-sm-7::text").get()
        property_type = response.css(".list-group-item:nth-child(5) .col-sm-7::text").get()
        square_meters= response.css(".list-group-item:nth-child(7) .col-sm-7::text").get()
        square_meters=int(extract_number_only(square_meters, '.', ','))
        available_date = response.css(".list-group-item:nth-child(4) .col-sm-7::text").get()
        images=response.xpath('//body[@class="immomakler_object-template-default single single-immomakler_object postid-43957 single-author cookies-not-accepted"]/div/div/div/div/div/div/div/img/@src').extract()
        terrace = balcony = pets_allowed = elevator = washing_machine = None
        for i in description :
            if 'platz' in i or 'plätze' in i or 'platz' in title or 'platz' in description.lower():
                parking = True
            if 'Terrasse' in i or 'Terrasse' in title or 'terrasse' in description.lower():
                terrace = True
            if 'Balkon' in i or 'Balkon' in title or 'balkon' in description.lower():
                balcony = True
            if 'Haustier' in i:
                pets_allowed = True
            if 'Waschmaschine' in i or 'Waschmaschine' in title or 'waschmaschine' in description.lower():
                washing_machine = True
            if 'Fahrstuhl/Aufzug' in i or 'Aufzug' in title or 'aufzug' in description.lower():
                elevator = True



        # if int(rent) <= 0 and int(rent) > 40000:
        #     return

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        #item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        #item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
        item_loader.add_value("currency", currency) # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
