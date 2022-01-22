# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class GbslLuebbeckeDeSpider(scrapy.Spider):
    name = "gbsl_luebbecke_de"
    start_urls = ['https://www.gbsl-luebbecke.de/immobilien-vermarktungsart/miete/']
    allowed_domains = ["gbsl-luebbecke.de"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
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
        urls = response.css('h3 a::attr(href)').extract()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.property-title::text').get()

        description = ' '.join(response.css('.property-description p::text').extract())

        loc = response.css('.property-subtitle::text').get().strip()
        longitude, latitude = extract_location_from_address(loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.css('img::attr(data-big)').extract()

        landlord_name = response.css('.fn::text').get().strip()
        landlord_number = response.css('.p-tel a::text').get().strip()
        landlord_email = response.css('.u-email a::text').get().strip()

        # info
        labels = [i.strip() for i in response.css('.property-details .col-sm-5::text').extract()]
        labels.insert(labels.index('Adresse') + 1, 'dummy')
        info = dict(zip(labels,
                        [i.strip() for i in response.css('.property-details .col-sm-7::text').extract() if i not in ['(Pr. Oldendorf)','(Lübbecke - Innenstadt)']]
                      ))
        floor = external_id = property_type = square_meters = deposit = heating_cost = utilities = energy_label = None
        room_count = bathroom_count = 1
        rent = 0
        for i in info.keys():
            if 'Objekt ID' in i:
                external_id = info[i]
            if 'Etage' in i:
                floor = info[i]
            if 'zimmer' in i.lower():
                room_count = int(float(info[i]))
            if 'Objekttyp' in i:
                if 'wohnung' not in info[i].lower():
                    return
                else:
                    property_type = 'apartment'
            if 'ohnfläche'.lower() in i.lower():
                square_meters = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'kaltmiete' in i.lower():
                rent = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Nebenkosten' in i:
                utilities = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Heizkosten' in i:
                heating_cost = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))

        details = [i.strip() for i in response.css('.property-features .list-group-item::text').extract()]

        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
        dishwasher = get_amenities(description, ' '.join(details), item_loader)

        energy_label = response.css('.property-epass .list-group-item:nth-child(9) .col-sm-7::text').get()

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if 0 >= int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        item_loader.add_value("floor", floor) # String
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
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
