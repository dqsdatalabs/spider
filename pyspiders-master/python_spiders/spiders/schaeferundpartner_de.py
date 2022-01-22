# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *

class SchaeferundpartnerDeSpider(scrapy.Spider):
    name = "schaeferundpartner_de"
    start_urls = ['https://www.schaeferundpartner.de/Mietangebote']
    allowed_domains = ["schaeferundpartner.de"]
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
        for url in ['https://www.schaeferundpartner.de/'+i for i in response.css('.hauptinfos a::attr(href)').extract()]:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Images
        images = ['https://www.schaeferundpartner.de/'+i for i in response.css('.focusview::attr(src)').extract()]

        # Title
        title = response.css('h1::text').get()

        # Description
        description = ' '.join(response.css('.active p::text').extract())
        description = re.sub(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                             '', description)
        description = re.sub(
            r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', '',
            description)

        description = re.sub(r'[\w.+-]+@[\w-]+\.[\w.-]+', '', description)

        description = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", description)
        description = re.sub(r"[\n\r]", " ", description)
        description = re.sub(r" +", " ", description)

        # longitude, latitude, zipcode, city, Address
        longitude, latitude = extract_location_from_address(response.css('h4+ div:nth-child(4)::text').get())
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # rent
        rent = int(response.xpath('//*[@id="inhalt"]/div[3]/div/div[2]/div[1]/div/div/div[3]/div[4]/span[2]/text()').get().split(',')[0].replace('.', ''))

        # square_meters
        square_meters = int(float(response.css('.col-sm-6:nth-child(2) .wert::text').get().split()[0].replace(',', '.')))

        # external_id
        external_id = response.css('.wert a::text').get()

        # Room_count
        room_count = int(float(response.css('.col-sm-6:nth-child(3) .wert::text').get()))

        # available_date
        available_date = None
        k = response.css('.links:nth-child(4) .wert::text').get()
        if '.' in k:
            if ' ' in k:
                available_date = '-'.join(k.split()[0].split('.')[::-1])
            else:
                available_date = '-'.join(k.split('.')[::-1])

        # Property_type
        property_type = property_type_lookup[response.css('h4+ div:nth-child(2)::text').get()]

        # details
        labels = response.css('.col-pt-6 .key::text').extract()
        values = response.css('.col-pt-6 .wert::text').extract()[0:3]+response.css('.col-pt-6 .fa').extract()+response.css('.col-pt-6 .wert::text').extract()[3:]
        label_vals = dict(zip(labels, values))
        # Balcony, terrace, pets_allowed, deposit, bathroom_count, floor, heating_cost, parking, elevator
        balcony = terrace = pets_allowed = deposit = bathroom_count = floor = heating_cost = parking = elevator = None
        if 'Kaution' in label_vals.keys():
            if ',' in label_vals['Kaution']:
                deposit = int(float(label_vals['Kaution'].split(',')[0].replace('.', '')))
            elif ' ' in label_vals['Kaution']:
                deposit = int(float(label_vals['Kaution'].split()[0].replace('.', '')))
        if 'Nebenkosten (ca.)' in label_vals.keys():
            heating_cost = int(float(label_vals['Nebenkosten (ca.)'].split(',')[0].replace('.', '')))
        if 'Badezimmer' in label_vals.keys():
            bathroom_count = int(float(label_vals['Badezimmer'].replace('.', '').replace(',', '.')))

        if 'Haustiere' in label_vals.keys():
            if 'fa-check' in label_vals['Haustiere']:
                pets_allowed = True
            else:
                pets_allowed = False
        if 'Balkon / Terrasse' in label_vals.keys():
            if 'fa-check' in label_vals['Balkon / Terrasse']:
                terrace = balcony = True
            else:
                terrace = balcony = False
        if 'Garage / Stellplatz' in label_vals.keys():
            if 'fa-check' in label_vals['Garage / Stellplatz']:
                parking = True
            else:
                parking = False

        if 'Etage' in label_vals.keys():
            floor = label_vals['Etage']
        if 'Aufzug' in label_vals.keys():
            elevator = True

        # dishwasher, washing_machine
        dishwasher = washing_machine = None
        if 'Geschirrspülmaschine' in description:
            dishwasher = True
        if 'Waschmaschine' in description:
            washing_machine = True

        # Landlord_info
        landlord_name = response.css('.kontaktname::text').get()
        landlord_number = response.css('.col-sm-12.col-lg-12::text').extract()[2].split('Telefon:')[1].strip()
        landlord_email = response.css('.col-lg-12 a::text').get()

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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
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
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
