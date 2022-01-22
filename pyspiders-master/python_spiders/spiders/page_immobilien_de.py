# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *
from parsel import Selector

class PageImmobilienDeSpider(scrapy.Spider):
    name = "page_immobilien_de"
    start_urls = ['https://smartsite2.myonoffice.de/kunden/weserems/1599/miete.xhtml?p[obj0]=']
    # allowed_domains = ["page-immobilien.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for i in [1, 2]:
            yield scrapy.Request(self.start_urls[0]+str(i), callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for url in ['https://smartsite2.myonoffice.de/kunden/weserems/1599/'+i for i in response.css('.obj-title a::attr(href)').extract()]:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response, rent=None):
        item_loader = ListingLoader(response=response)

        title = response.css('h1::text').get()

        landlord_email = [i.strip() for i in response.css('.contact-info span::text').extract()][3]
        landlord_number = [i.strip() for i in response.css('.contact-info span::text').extract()][4].split('Tel. Zentrale ')[1]
        landlord_name = response.css('.name strong::text').get().strip()

        images = response.css('img::attr(src)').extract()

        text = response.css(".row").get()
        s = Selector(text)
        images = s.css('div::attr(data-img)').getall()

        bathroom_count = room_count = 1
        property_type = 'apartment'
        square_meters = utilities = energy_label = deposit = None
        rent = 0
        info = dict(zip(response.css('.data-2col strong::text').extract(), response.css('.data-2col span::text').extract()[1:]))
        for i in info.keys():
            if 'Objektart' in i:
                if 'Büro/Praxen' in info[i]:
                    return
            if 'Energieeffizienzklasse' in i:
                energy_label = info[i]
            if 'Anzahl Zimmer' in i:
                room_count = int(float(info[i].replace('.', '').replace(',', '.')))
            if 'Kaltmiete' in i:
                rent = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'NEBENKOSTEN'.lower() in i.lower():
                utilities = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Wohnfläche' in i:
                square_meters = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))

        loc = [i.strip() for i in response.css('.contact-info span::text').extract()][1]
        longitude, latitude = extract_location_from_address('Moltkestraße 8 '+loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        description = ' '.join(response.css('.obj-description span::text').extract()).strip()

        details = ' '.join(response.css('.freetext span span::text').extract()).strip()

        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
            dishwasher = get_amenities(description, details, item_loader)

        if 0 >= int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
