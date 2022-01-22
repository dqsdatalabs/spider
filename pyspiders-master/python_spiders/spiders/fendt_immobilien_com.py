# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class FendtImmobilienComSpider(scrapy.Spider):
    name = "fendt_immobilien_com"
    start_urls = ['http://site7.fendt.netcore.web2.onoffice.de/mietobjekte.xhtml']
    # allowed_domains = ["fendt-immobilien.com"]
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
        for url in ['http://site7.fendt.netcore.web2.onoffice.de/'+i for i in response.css('.link::attr(href)').extract()]:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1::text').get()

        landlord_email = response.css('a::attr(href)').get().split(':')[1]
        landlord_number = response.css('span:nth-child(1) span span::text').get()
        landlord_name = response.css('.name::text').get()

        image = response.xpath('/html/body/div/div/div/div[3]/div[1]/div/div[*]/@data-img').extract()

        bathroom_count = room_count = 1
        property_type = 'apartment'
        warm_rent = utilities = exteranl_id = square_meters = heating_cost = energy_label = deposit = None
        rent = 0
        city = zipcode = ''

        info = dict(zip(response.css('td strong::text').getall(),response.css('.datablock-half span::text').getall()))

        for i in response.css('.freetext p::text').getall():
            if 'Klasse' in i:
                energy_label = i.split(':')[1].strip()

        for i in info.keys():
            if 'Objektart' in i:
                if 'Büro/Praxen' in info[i]:
                    return

            if 'PLZ' in i:
                zipcode = info[i]

            if 'Ort' in i:
                city = info[i]

            if 'ETAGE'.lower() in i.lower():
                if 'Dachgeschos' in info[i]:
                    floor = '0'
                else:
                    floor = info[i]

            if 'Kaltmiete' in i:
                rent = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))

            if 'Kaution' in i:
                if info[i].split()[1]=='KM':
                    deposit = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))*rent
                else:
                    deposit = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))

            if 'NEBENKOSTEN'.lower() in i.lower():
                utilities = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))

            if 'Warmmiete'.lower() in i.lower():
                warm_rent = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))

            if 'externe Objnr' in i:
                exteranl_id = info[i]

        heating_cost = warm_rent - (rent+utilities)

        extras = response.css('.dimtext::text').get().split()
        for i in range(len(extras)):
            if 'm²' in extras[i]:
                square_meters = int(float(extras[i-1].replace('.', '').replace(',', '.')))
            if 'Zimmer' in extras[i]:
                room_count = int(float(extras[i - 1].replace('.', '').replace(',', '.')))

        loc = '83435'+' '+'Bad Reichenhall'
        longitude, latitude = extract_location_from_address(loc)
        _, city, address = extract_location_from_coordinates(longitude, latitude)

        description = ' '.join(response.css('.freetext span span::text').extract()).strip()

        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
        dishwasher = get_amenities(description, '', item_loader)

        description = description_cleaner(description)

        if 0 >= int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", exteranl_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", image)  # Array
        item_loader.add_value("external_images_count", len(image))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()