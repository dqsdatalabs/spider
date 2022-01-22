# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import math

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class blankschmackimmobilien(scrapy.Spider):
    name = "blankschmackimmobilien"
    start_urls = ['https://www.blank-schmack-immobilien.de/immobilien/wohnen/mieten/wohnungen?lat=&lng=&city=&district=&radius=0']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls= response.xpath('//a[@class="no-hover"]/@href').extract()
        for x in range(len(urls)):
            url = "https://www.blank-schmack-immobilien.de"+urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)
        page="https://www.blank-schmack-immobilien.de"+"".join(response.xpath('//ul[@class="pagination pagination-centered pull-left"]/li/a/@href').extract()[1:])
        yield scrapy.Request(url=page, callback=self.parse)



    # 3. SCRAPING level 3
    def populate_item(self, response):
        room_count = None
        bathroom_count = None
        floor = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        utilities = None
        terrace = None
        furnished = None
        property_type = None
        energy_label = None
        deposit = None
        square_meters = None
        available = None
        heating_cost = None
        external_id = None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//div[@class="expose"]/h1/text()').extract()).strip()
        l1 = response.xpath('//*[@class="right-box"][2]/table/tr/td/text()').extract()
        l1 = [x.replace("  ", "").strip() for x in l1]
        l1 = [x for x in l1 if x]
        l2 = response.xpath('//*[@class="right-box"][2]/table/tr/td/span/text()').extract()
        try :
            l1.remove("Heizkosten in Nebenkosten enthalten")
        except:
            pass

        l = dict(zip(l1, l2))
        l3 = response.xpath('//*[@class="right-box"][4]/table/tr/td/text()').extract()
        l3 = [x.replace("  ", "").strip() for x in l3]
        l3 = [x for x in l3 if x]
        l4=response.xpath('//*[@class="right-box"][4]/table/tr/td/span/text()').extract()
        ll=dict(zip(l3, l4))
        try:
            rent = int(float(l.get("Miete zzgl. NK").replace(" €", "")))
        except:
            try :
                rent = int(float(l.get("Kaltmiete").replace(" €", "")))
            except:
                return

        try:
            external_id = l.get("Objekt-Nr.")
        except:
            pass

        try:
            bathroom_count = int(float(l.get("Badezimmer")))
        except:
            bathroom_count=1
        try:
            square_meters = int(float(l.get("Wohnfläche").replace("ca.\xa0", "").replace("\xa0m²","").replace(",",".")))
        except:
            pass
        try:
            deposit = int(float(l.get("Kaution").replace(" €", "")))
        except:
            pass
        try:
            room_count = math.ceil(float(l.get("Zimmer").replace(",",".")))
        except:
            pass
        try:
            heating_cost = int(float(l.get("Heizkosten").replace(" €", "")))
        except:
            pass
        try:
            if "Ausstattung" in "".join(response.xpath('//div[@class="property-right"]/p/strong/text()').extract()):
                furnished = True
        except:
            pass
        try:
            floor = ll.get("Lage im Objekt (Etage)")
        except:
            pass
        try:
            utilities = int(float(l.get("Nebenkosten").replace(" €", "")))
        except:
            pass

        try:
            available = ll.get("verfügbar ab")
        except:
            pass
        extras = "".join(response.xpath('//*[@id="main"]/div[3]/div[4]/div/div/ul/li/text()').extract())
        description = "".join(response.xpath('//div[@class="expose"]//descendant::text()').extract()).strip()
        description=remove_white_spaces(description)
        description = description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(
            description, description, item_loader)
        images = response.xpath('//*[@class="embed-responsive embed-responsive-4by3"]/a/@href').extract()

        landlord_name = "Blank & Schmack Immobilien"
        landlord_number = "+49 203 3018960"
        try:
            en=response.xpath('//*[@class="right-box"][3]/table/tr/td/span/text()').extract()
            energy_label=en[en.index("Energieeffizienzklasse")+1].strip()
        except:
            pass
        address = l.get("Adresse")
        property_type="apartment"
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
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
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available)  # String => date_format

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
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        # item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
