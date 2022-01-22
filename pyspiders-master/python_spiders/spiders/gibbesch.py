# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import math

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class gibbesch(scrapy.Spider):
    name = "gibbesch"
    start_urls = ["https://www.gibbesch.de/immobilie/alle-immobilien/"]
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
        urls= response.xpath('//h3[@class="property-title"]/a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item,dont_filter=True)


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
        external_id =None
        pets_allowed=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//*[@id="post-91"]/div/div[2]/div/div/div/div/h2/text()').extract()).strip()
        l1=response.xpath('.//div[@class="dt col-sm-5"]/text()').extract()
        l1=[x.replace("  ","").strip() for x in l1 ]
        l1=[x for x in l1 if x ]
        l2=response.xpath('.//div[@class="dd col-sm-7"]/text()').extract()
        l=dict(zip(l1,l2))
        try :
            external_id=l.get("Objekt ID")
        except:
            pass
        try:
            rent =int(float(l.get("Kaltmiete").replace("00\u202fEUR","").replace(",",".").replace(".","")))
        except:
            return
        try:
            bathroom_count = int(float(l.get("Badezimmer")))
        except:
            bathroom_count =1
        try:
            square_meters = int(float(l.get("Wohnfläche\xa0ca.").replace("\u202fm²","")))
        except:
            pass
        try:
            deposit= int(float(l.get("Kaution").replace("00\u202fEUR","").replace(".","").replace(",",".")))
        except:
            pass
        try:
            room_count = math.ceil(float(l.get("Zimmer").replace(",",".")))
        except:
            room_count=1

        try :
            floor=l.get("Etage")
        except:
            pass
        try:
            utilities = int(float(l.get("Kaltmiete").replace("00\u202fEUR","").replace(",",".")))
        except:
            pass

        try :
            available=l.get("Verfügbar ab")
        except:
            pass
        extras = "".join( response.xpath('//div[@class="panel-body"]/ul/li/text()').extract()).strip()
        description = "".join(response.css("div[class='panel-body'] p ::text").getall()).strip()
        description=description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,extras,item_loader)
        if "pkw-stellplatzdie" in description.lower():
            parking=True
        if "Terrassen" in l1:
            terrace = True
        if "Stellplätze gesamt" in l1:
            parking = True
        images = response.xpath('.//div[@id="immomakler-galleria"]/a/@href').extract()
        landlord_name="".join(response.xpath('.//span[@class="p-name fn"]/text()').extract()).strip()
        if landlord_name =="" :
            landlord_name="Gibbesch"
        landlord_number ="".join(response.xpath('.//div[@class="dd col-sm-13 p-tel value"]/a/text()').extract()).strip()
        if landlord_number == "":
            landlord_number = "04532/26306-0"
        landlord_email = "".join(response.xpath('.//div[@class="dd col-sm-13 u-email value"]/a/text()').extract()).strip()
        if landlord_email == "":
            landlord_email = "immobilien@gibbesch.de"
        l4=response.xpath('.//div[@class="dt col-xs-5"]/text()').extract()
        l3=response.xpath('.//div[@class="dd col-xs-7"]/text()').extract()
        l5=dict(zip(l4,l3))
        try:
            energy_label=l5.get("Energie\xadeffizienz\xadklasse")
        except:
            pass
        address = l.get("Adresse").replace("\xa0",",")
        pro= l.get("Objekttypen")
        if "wohnung" in pro.lower() :
            property_type="apartment"
        elif "haus" in pro.lower() :
            property_type="house"
        else :
            return
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
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
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

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
