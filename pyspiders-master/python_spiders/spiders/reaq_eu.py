# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ReaqEuSpider(scrapy.Spider):
    name = "reaq_eu"
    start_urls = ['https://www.reaq.eu/mietwohnungen']
    allowed_domains = ["reaq.eu"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        for url in ['https://www.reaq.eu/'+i for i in response.css('#article-64 a::attr(href)').extract()]:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # images
        images = ['https://www.reaq.eu/'+i for i in response.xpath('//*[@id="slider"]/div[1]/div[*]/img/@src').extract()]

        # title
        title = response.css('h1 > span::text').get()

        # longitude, latitude, zipcode, city, address
        longitude, latitude = extract_location_from_address(response.css('.object-info-address div+ div::text').get())
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        if city == 'None':
            city = 'Aachen'
        # description
        available_date = None
        description = ' '.join(response.css('.object-free-text::text').extract()).strip()
        if re.search("([0-9]{2}\.[0-9]{2}\.[0-9]{4})", description):
            available_date = '-'.join(re.search("([0-9]{2}\.[0-9]{2}\.[0-9]{4})", description)[0].split('.')[::-1])
        description = description_cleaner(description)

        # features
        info = dict(zip(response.css('td:nth-child(1)::text').extract(), response.css('td:nth-child(2) strong::text').extract()))

        # rent, deposit, room_count, square_meters, external_id, heating_cost
        rent = deposit = room_count = square_meters = external_id = heating_cost = None
        for i in info.keys():
            if 'Kaltmiete' in i:
                rent = int(float(info[i].split()[1].replace('.', '').replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(info[i].split()[1].replace('.', '').replace(',', '.')))
            if 'Wohnfläche' in i:
                square_meters = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Ref.-Nr.' in i:
                external_id = info[i]
            if 'Zimmer' in i:
                room_count = int(float(info[i].replace(',','.')))
            if 'Nebenkosten' in i:
                heating_cost = int(float(info[i].split()[1].replace('.', '').replace(',', '.')))

        # details
        details = ' '.join(response.css('.object-info-facilities div::text').extract())

        # terrace, balcony, elevator
        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine = dishwasher = None

        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities (description, details, item_loader)


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
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

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
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'anfrage reaq') # String
        item_loader.add_value("landlord_phone", '0241/4040370') # String
        item_loader.add_value("landlord_email", 'anfrage@reaq.eu') # String

        self.position += 1
        yield item_loader.load_item()
