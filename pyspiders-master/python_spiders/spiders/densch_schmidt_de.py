# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class DenschSchmidtDeSpider(scrapy.Spider):
    name = "densch_schmidt_de"
    start_urls = ['https://www.densch-schmidt.de/suche/?tx_mbimmoobjects_searchresult%5B%40widget_0%5D%5BcurrentPage%5D=7&cHash=44c5445cf2db49252a3d4363f1298acb']
    allowed_domains = ["densch-schmidt.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    t = []
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_page)

    # 2. SCRAPING level 2
    def parse_page(self, response, **kwargs):
        pages = ['https://www.densch-schmidt.de'+i for i in response.xpath('//*[@id="c263"]/div/div/div/div[2]/nav/ul/li[*]/a/@href').extract()]
        pages.append(response.url)
        for page in pages:
            yield scrapy.Request(page, callback=self.parse)

    # 3. SCRAPING level 3
    def parse(self, response, **kwargs):
        for prop in ['https://www.densch-schmidt.de'+i for i in response.xpath('//*[@id="c263"]/div/div/div/div[2]/div[*]/div[2]/div/h3/a/@href').extract()]:
            yield scrapy.Request(prop, callback=self.populate_item)

    # 4. SCRAPING level 4
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        self.t.append(response.url)
        # Details
        labels = [i.strip() for i in response.css('th::text').extract()]
        vals = [i.strip() for i in response.css('td::text').extract()]
        label_vals = dict(zip(labels, vals))

        if 'Kaltmiete' not in label_vals.keys():
            return
        if 'Objektart' in label_vals.keys():
            return

        # rent
        rent = int(label_vals['Kaltmiete'].split(',')[0].replace('.', ''))

        # square meters
        square_meters = None
        if 'Wohnfläche' in label_vals.keys():
            square_meters = int(label_vals['Wohnfläche'].split(',')[0].replace('.', ''))
        elif 'Nutzfläche' in label_vals.keys():
            square_meters = int(label_vals['Nutzfläche'].split(',')[0].replace('.', ''))

        # room_count
        room_count = 1
        if 'Zimmer' in label_vals.keys():
            room_count = int(float(label_vals['Zimmer']))

        # floor
        floor = None
        if 'Etage' in label_vals.keys():
            if re.search(r'\d', label_vals['Etage']):
                floor = re.search(r'\d', label_vals['Etage'])[0]


        # available_from
        available_date = None
        if 'Frei ab' in label_vals.keys():
            available_date = '-'.join(label_vals['Frei ab'].split('.')[::-1])

        # utilities
        utilities = None
        if 'Nebenkosten' in label_vals.keys():
            utilities = int(label_vals['Nebenkosten'].split(',')[0].replace('.', ''))

        # heating_cost
        heating_cost = None
        if 'Heizkosten' in label_vals.keys():
            heating_cost = int(label_vals['Heizkosten'].split(',')[0].replace('.', ''))

        # Description
        description = ''.join(response.css('.info p::text').extract())
        description = re.sub(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                             '', description)
        description = re.sub(
            r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', '',
            description)
        description = re.sub(r'[\w.+-]+@[\w-]+\.[\w.-]+', '', description)

        description = re.sub(r"[\n\r]", " ", description)

        # deposit
        deposit = None
        if re.search(r'Kaution:', description):
            kaution = description.split('Kaution:')[1]
            test_str = re.sub(r"[_.*+(){}';@#?!&$/-]+\ *", "", kaution)

            if re.search(r'\d+', test_str):
                kau = re.search(r'\d+', test_str)[0]

                if len(kau)>1:
                    deposit = int(kau.replace('.',''))
                else:
                    deposit = int(kau)*rent

        description = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", description)
        description = re.sub(r" +", " ", description)

        # Images
        images = ['https://www.densch-schmidt.de'+i for i in response.css('.lightbox img::attr(src)').extract()]

        # title
        title = response.css('h1::text').get()

        # external id
        external_id = response.css('.object-no::text').get().strip().split(': ')[1]

        # balcony , terrace , pets_allowed , parking ,elevator, dishwasher, washing_machine
        balcony = terrace = pets_allowed = parking = elevator = None
        dishwasher = washing_machine = None
        if 'haustiere' in description.lower():
            pets_allowed = True

        if 'balkon' in description.lower():
            balcony = True

        if 'terrasse' in description.lower():
            terrace = True

        if 'garage' in description.lower():
            parking = True

        if 'stellplätze' in description.lower():
            parking = True

        if 'aufzug' in description.lower():
            elevator = True

        if 'geschirrspülmaschine' in description.lower():
            dishwasher = True

        if 'waschmaschine' in description.lower():
            washing_machine = True

        # Furnished
        furnished = None
        if 'ausstattung' in description.lower():
            furnished = True

        # property_type
        property_type = 'apartment'


        # longitude, latitude, zipcode, city, address
        loc = response.css('.location').get()
        start_ind = re.search(r'\d', response.css('.location').get()).start()
        loc = loc[start_ind:]
        end_ind = re.search(r'\n', loc).start()
        loc = loc[:end_ind]
        longitude, latitude = extract_location_from_address(loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # Landlord_info
        landlord_name = response.css('.name::text').get()
        landlord_number = response.css('.tel::text').get().strip()
        landlord_email = 'flensburg@densch-schmidt.de'





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
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
        item_loader.add_value("utilities", utilities) # Int
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
