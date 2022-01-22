# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class wgf(scrapy.Spider):
    name = "wgf"
    start_urls = ['https://wgf.de/search-results/?bedrooms=&min-price=100&max-price=2000']
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
        urls= response.xpath('//div[@class="listing-thumb"]/a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)



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
        square_meters=None
        available=None
        heating_cost=None
        zipcode=None
        city=None
        floor_plan_images=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//*[@class="page-title"]/h1/text()').extract()).strip()
        address = "".join(response.xpath('//*[@class="item-address"]/text()').extract()).strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
        l2=response.xpath('//*[@class="list-2-cols list-unstyled"]/li/span/text()').extract()
        l1=response.xpath('//*[@class="list-2-cols list-unstyled"]/li/strong/text()').extract()
        l=dict(zip(l1,l2))
        try:
            rent =int(float(l.get('Kaltmiete:').replace("€","").replace(",",".")))
        except:
            try :
                rent =int(float(l.get('Preis pro Monat:').replace("€","").replace(",",".")))
            except:
                return

        external_id="".join(response.xpath('//*[@id="property-overview-wrap"]/div/div[1]/div/text()').extract()).strip()

        try:
            bathroom_count = int(float(l.get('Badezimmer:').replace(",",".")))
        except:
            bathroom_count=1
        try:
            square_meters = int(float(l.get('Fläche ca.:').replace(",",".").replace(" m²","").replace("ca. ","")))
        except:
            pass
        try:
            deposit  =int(float(l.get('Kaution:').replace("€","").replace(",",".")))
        except:
            pass
        try:
            room_count = int(float(l.get('Zimmer:').replace(",",".")))
        except:
            pass
        try:
            heating_cost =int(float(l.get('monatl. Heizkosten:').replace("€","").replace(",",".")))
        except:
            pass
        try :
            floor=l.get("Etage:")
        except:
            pass
        try:
            utilities =int(float(l.get('monatl. Betriebs-/Nebenkosten:').replace("€","").replace(",",".")))
        except:
            pass
        try :
            available=l.get('Immobilie ist verfügbar ab:')
        except:
            pass
        extras = "".join(response.xpath('//div[@class="property-features"]/ul/li/text()').extract())
        description = response.xpath('//div[@class="block-content-wrap"]/p/text()').extract()
        try:
            floor_plan_images=response.xpath('//div[@class="accordion-body"]/a/@href').extract()
        except :
            pass
        description="".join(description)
        description=description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        images=response.xpath('//*[@id="property-gallery-js"]/div/a/img/@src').extract()
        landlord_name="Wohnungsgesellschaft der Stadt Finsterwalde"
        landlord_number="(03531) 79 15 0"
        landlord_email="info@wgf.de"
        pro=l.get('Typ:')
        if "wohnung" in pro.lower() :
            property_type="apartment"
        else:
            property_type="house"
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
        item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
