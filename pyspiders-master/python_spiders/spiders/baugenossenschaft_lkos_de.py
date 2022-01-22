# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class BaugenossenschaftLkosDeSpider(scrapy.Spider):
    name = "baugenossenschaft_lkos_de"
    start_urls = ['https://baugenossenschaft-lkos.de/aktuelle-angebote/?post_type=immomakler_object&paged=1&vermarktungsart=miete&nutzungsart=wohnen&typ=&ort=&center=&radius=500&objekt-id=&collapse=&von-qm=0.00&bis-qm=460.00&von-zimmer=0.00&bis-zimmer=10.00&von-kaltmiete=0.00&bis-kaltmiete=2000.00']
    allowed_domains = ["baugenossenschaft-lkos.de"]
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
        props = [re.search(r'"(.*)"', i)[0].replace('"', '') for i in [re.search(r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', i)[0] for i in response.css('.property-title').extract()]]
        for url in props:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # images
        images = [i.split()[-2].split()[0] for i in response.css('#immomakler-galleria img::attr(srcset)').extract()]

        # title
        title = response.css('.property-title::text').get()

        # landlord info
        landlord_name = 'Lisa Borgmann'
        landlord_number = '05464 96707-0'
        landlord_email = 'info@bglo.de'

        # info
        info = dict(zip(response.css('.property-details .col-sm-5::text').extract(), response.css('.property-details .col-sm-7::text').extract()[:2]+response.css('.property-details .col-sm-7::text').extract()[3:]))
        external_id = floor = warm_rent = rent = deposit = utilities = heating_cost = square_meters = available_date = bathroom_count = None
        room_count = 1
        property_type = 'apartment'
        for i in info.keys():
            if 'Objekt ID' in i:
                external_id = info[i]
            if 'Etage' in i:
                if 'EG' in info[i] or 'Erdgeschoß' in info[i] or '1' in info[i]:
                    floor = '1'
                if '1.OG' in info[i] or '2' in info[i]:
                    floor = '2'
                if '2.OG' in info[i] or '3' in info[i]:
                    floor = '3'
                if '3.OG' in info[i] or '4' in info[i]:
                    floor = '4'

            if 'Kaltmiete' in i:
                rent = int(float(info[i].split(',')[0].replace('.','')))
            if 'Kaution' in i:
                deposit = int(float(info[i].split(',')[0].replace('.','')))
            if 'Nebenkosten' in i:
                utilities =  int(float(info[i].split(',')[0].replace('.','')))
            if 'Heizkosten' in i:
                heating_cost = int(float(info[i].split(',')[0].replace('.','')))
            if 'Warmmiete' in i:
                warm_rent = int(float(info[i].split(',')[0].replace('.','')))
            if 'Wohnfläche' in i:
                square_meters = int(float(re.search(r'\d+', info[i].replace('.',''))[0]))
            if 'Objekttypen' in i:
                for j in property_type_lookup.keys():
                    if j in info[i]:
                        property_type = property_type_lookup[j]
            if 'Zimmer' in i:
                room_count = int(float(info[i].strip()))
            if 'Badezimmer' in i:
                bathroom_count = int(float(info[i].strip()))

            if re.search(r'\d+\.\d+\.\d+', info[i]):
                available_date = '-'.join(re.search(r'\d+\.\d+\.\d+', info[i])[0].split('.')[::-1])
        if heating_cost is None and warm_rent is not None:
            heating_cost = warm_rent-rent

        # longitude, latitude, zipcode, city, address
        longitude, latitude = extract_location_from_address(info['Adresse'].replace('\xa0', ' '))
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        if zipcode == address:
            address = response.css('.property-details .col-sm-7::text').extract()[2].replace('\xa0', ' ') +' '+ city

        # description
        description = response.css('.property-description p::text').get()
        description = description_cleaner(description)

        # energy info
        energy_info = dict(zip(response.css('.col-xs-5::text').extract(), response.css('.col-xs-7::text').extract()))
        energy_label = None
        for i in energy_info.keys():
            if 'klasse' in i:
                energy_label = energy_info[i]
            elif 'Endenergiebedarf' in i:
                energy_label = energy_label_extractor(int(float(energy_info[i].split('kWh')[0].replace(',', '.'))))

        # details
        details =' '.join(response.css('.property-features .list-group-item::text').extract())

        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, details, item_loader)

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
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
