# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *

class InsiderImmobilienMsDeSpider(scrapy.Spider):
    name = "insider_immobilien_ms_de"
    start_urls = ['https://www.insider-immobilien-ms.de/angebote/mieten.php']
    allowed_domains = ["insider-immobilien-ms.de"]
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
        urls = response.css('.immo_btnObject::attr(href)').extract()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        title = response.css('.immo_headlineDetail::text').get().strip()
        loc = response.xpath('//*[@id="immo_detail"]/div[2]/div[1]/div/dl[2]/dd/address/span/text()').get().strip()
        longitude, latitude = extract_location_from_address(loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        description = response.css('.row:nth-child(4) .col::text').get()

        images = response.xpath('//*[@id="immo_coverSlider"]/div[1]/div[*]/a/img/@src').extract()

        room_count = 1
        property_type = 'apartment'

        info = dict(zip([i.strip() for i in response.css('.immo_marginBottom dt::text').extract() if i not in ['Adresse der Immobilie']], [i.strip() for i in response.css('.immo_marginBottom dd::text').extract()]))
        available_date = None
        bathroom_count = 1
        landlord_name=None

        for i in info.keys():
            if 'wohnfläche' in i.lower():
                square_meter = int(float(info[i].split()[0].replace('.', '').replace(',', '.')))
            if 'objektnummer' in i.lower():
                external_id = info[i]
            if 'ansprechpartner' in i.lower():
                landlord_name = info[i]
            if 'kaution' in i.lower():
                deposit = int(float(info[i].split('\xa0')[0].replace('.', '').replace(',', '.')))
            if 'warmmiete' in i.lower():
                warm = int(float(info[i].split('\xa0')[0].replace('.', '').replace(',', '.')))
            if 'kaltmiete netto' in i.lower():
                rent = int(float(info[i].split('\xa0')[0].replace('.', '').replace(',', '.')))
            if 'verfügbar ab' in i.lower():
                if '.' in info[i]:
                    if 'ab' in info[i]:
                        available_date = '-'.join(info[i].split()[1].split('.')[::-1])
                    else:
                        available_date = '-'.join(info[i].split('.')[::-1])


            if 'effizienzklasse' in i.lower():
                energy_label = info[i]
            if 'nebenkosten' in i.lower():
                utilities = int(float(info[i].split('\xa0')[0].replace('.', '').replace(',', '.')))
            if 'zimmer' in i.lower():
                room_count = int(float(info[i]))
            if 'badezimmer' in i.lower():
                bathroom_count = int(float(info[i]))
            if 'objektart' in i.lower():
                if info[i] not in property_type_lookup.keys():
                    return
                else:
                    property_type = property_type_lookup[info[i]]

        details = response.css('.row:nth-child(5) .col::text').extract()

        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
        dishwasher = get_amenities(description, ' '.join(details)+''+' '.join([i.strip() for i in response.css('dt::text').extract()]), item_loader)

        description = description_cleaner(description)

        heating_cost = warm-rent

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if 0 >= int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
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
                              'apartment')  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meter)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

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

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", '0251 287 45 71')  # String
        item_loader.add_value("landlord_email", 'im@insider-immobilien-ms.de')  # String

        self.position += 1
        yield item_loader.load_item()
