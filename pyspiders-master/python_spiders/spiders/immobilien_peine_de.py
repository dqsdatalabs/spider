# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ImmobilienPeineDeSpider(scrapy.Spider):
    name = "immobilien_peine_de"
    start_urls = ['https://portal.immobilienscout24.de/ergebnisliste/90199744']
    #allowed_domains = ["immobilien-peine.de"]
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
        for url in list(
                set(['https://portal.immobilienscout24.de' + i for i in response.css('a::attr(href)').extract() if
                     'expose' in i])):
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.is24__block__responsive--col1 h4::text').get().strip()
        description = ' '.join([i.strip() for i in response.css('.expose--text~ .expose--text+ .expose--text:nth-child(6) p::text').extract() if i.strip() != ''])

        loc = response.css('.expose--text__address p::text').get().strip()
        longitude, latitude = extract_location_from_address(loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = ['https:' + i.split('/ORIG')[0] for i in response.css('.sp-image::attr(data-src)').extract()]
        external_id = response.css('.form+ .expose--text p:nth-child(1)::text').get().split()[1]

        # info
        labels = [i for i in response.css('li p:nth-child(1)::text').extract() if i not in ['Balkon/ Terrasse:', 'Keller:', 'Einbauküche:', 'Energieverbrauch für Warmwasser enthalten:']]
        vals = response.css('li p+ p::text').extract()
        property_type = 'apartment'
        available_date = floor = square_meters = deposit = heating_cost = utilities = energy_label = None
        room_count = bathroom_count = 1
        rent = 0
        kt = 1
        for i in range(len(vals)):
            if 'Energieeffizienzklasse:' in labels[i]:
                energy_label = vals[i]
            if 'Zimmer' in labels[i]:
                room_count = int(float(vals[i].replace(',', '.')))
            if 'badezimmer' in labels[i].lower():
                bathroom_count = int(float(vals[i].replace(',', '.')))
            if 'Bezugsfrei ab' in labels[i]:
                if '.' in vals[i]:
                    available_date = '-'.join(vals[i].split('.')[::-1])
            if 'ohnfläche'.lower() in labels[i].lower():
                square_meters = int(float(vals[i].split()[0].replace('.', '').replace(',', '.')))
            if 'kaltmiete' in labels[i].lower():
                rent = int(float(vals[i].replace('+', '').strip().split()[0].replace('.', '').replace(',', '.')))
            if 'Nebenkosten:' in labels[i]:
                utilities = int(float(vals[i].replace('+', '').strip().split()[0].replace('.', '').replace(',', '.')))
            if 'Heizkosten:' in labels[i]:
                heating_cost = int(
                    float(vals[i].replace('+', '').strip().split()[0].replace('.', '').replace(',', '.')))
            if 'Kaution' in labels[i]:
                if 'Kaltmieten' in vals[i]:
                  deposit = int(float(vals[i].split()[0]))*rent
                else:
                  deposit = int(float(vals[i].replace('+', '').strip().replace('.', '').replace(',', '.')))


        details = response.css('h4+ p::text').extract()
        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
        dishwasher = get_amenities(description, ' '.join(details) + ' '.join(labels), item_loader)


        description = description_cleaner(description)

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
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date) # String => date_format

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
        item_loader.add_value("landlord_name", 'Herr Sascha Ohligschläger')  # String
        item_loader.add_value("landlord_phone", '05171 290890')  # String
        item_loader.add_value("landlord_email", 'sascha.o@t-online.de')  # String

        self.position += 1
        yield item_loader.load_item()