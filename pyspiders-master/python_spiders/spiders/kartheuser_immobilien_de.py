# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class KartheuserImmobilienDeSpider(scrapy.Spider):
    name = "kartheuser_immobilien_de"
    start_urls = ['https://kartheuser-immobilien.de/immobilien?post_type=immomakler_object&vermarktungsart=miete&nutzungsart=wohnen&typ&ort&collapse&von-qm=0.00&bis-qm=955.00&von-zimmer=0.00&bis-zimmer=30.00&von-kaltmiete=0.00&bis-kaltmiete=1000.00&von-kaufpreis=0.00&bis-kaufpreis=6275000.00']
    allowed_domains = ["kartheuser-immobilien.de"]
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
        for url in response.css('.property-title a::attr(href)').extract():
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.property-title::text').get()
        loc = response.css('.property-subtitle::text').get().strip()
        longitude, latitude = extract_location_from_address('Moltkestraße 8 ' + loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.xpath('//*[@id="immomakler-galleria"]/a[*]/@href').getall()

        landlord_email = response.xpath('//*[@id="inner-content-container"]/div[2]/div[2]/div[1]/div[3]/div[2]/div/div[2]/ul/li[2]/div/div[2]/a/@href').get().split(':')[1]
        landlord_number = response.xpath('//*[@id="inner-content-container"]/div[2]/div[2]/div[1]/div[3]/div[2]/div/div[2]/ul/li[3]/div/div[2]/a/@href').get().split('://')[1]
        landlord_name = response.xpath('//*[@id="inner-content-container"]/div[2]/div[2]/div[1]/div[3]/div[2]/div/div[2]/ul/li[1]/div/div[2]/span/text()').get()

        description = response.xpath('//*[@id="inner-content-container"]/div[2]/div[2]/div[4]/div/div[2]/p[1]/text()').get()
        details = ' '.join(response.css('.property-description .panel-body p::text').getall()).strip()
        extra_features = ' '.join(response.css('.property-features .list-group-item::text').getall())
        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
        dishwasher = get_amenities(description, details +' '+extra_features, item_loader)

        floor_plan_images = response.css('.example-image::attr(src)').getall()



        energy_info = dict(zip(response.css('.property-epass .col-sm-5::text').getall(), response.css('.property-epass .col-sm-7::text').getall()))
        energy_label = None
        for i in energy_info.keys():
            if 'kalsse' in i.lower():
                energy_label = energy_info[i]

        info = dict(zip(response.css('.property-details .col-sm-5::text').getall() , [i.strip() for i in response.css('.property-details .col-sm-7::text').getall() if i.split(':')[0].strip() not in [ "Fernbahnhof", "Autobahn", "Bus"] and i.strip() !="" and 'Velbert' not in i and 'Westfalen' not in i and 'Wülfrath' not in i]))

        bathroom_count = room_count = 1
        property_type = 'apartment'
        floor = availability_date = heat_cost = square_meters = utilities = energy_label = deposit = None
        rent = 0

        if 'Kaltmiete' not in info.keys():
            return
        for i in info.keys():
            if 'Objekt ID' in i:
                external_id = info[i]
            if 'Etage' in i:
                floor = info[i]
            if 'Zimmer' in i:
                room_count = int(float(info[i].replace('.', '').replace(',', '.')))
            if 'Kaltmiete' in i:
                rent = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'NEBENKOSTEN'.lower() in i.lower():
                heat_cost = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Wohnfläche' in i:
                square_meters = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Ausstattung' in i:
                furnished = True
            if 'Verfügbar ab' in i:
                if '.' in info[i]:
                    availability_date = '-'.join(info[i].split('.')[::-1])


        description = description_cleaner(description)

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return

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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", availability_date) # String => date_format

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
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heat_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
