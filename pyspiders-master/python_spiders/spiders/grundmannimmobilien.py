# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import math

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class hermannimmobilien(scrapy.Spider):
    name = "grundmannimmobilien"
    start_urls =["https://smartsite2.myonoffice.de/kunden/grundmannimmo/15/immobilien.xhtml"]
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
        urls= response.xpath('//div[@class="obj-title"]/a/@href').extract()
        for x in range(len(urls)):
            url = "https://smartsite2.myonoffice.de/kunden/grundmannimmo/15/"+urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)
        pages=response.xpath('//div[@class="obj-bottom-frame"]/div/div/span/a/@href').extract()[1:]
        for y in pages :
            url = "https://smartsite2.myonoffice.de/kunden/grundmannimmo/15/" + y
            yield scrapy.Request(url=url, callback=self.parse)


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
        title = "".join(response.xpath('//*[@class="obj-detail"]/div[1]/div[1]/h1/text()').extract()).strip()
        l1=response.xpath('//*[@class="obj-detail"]/div[2]/div[1]/div[2]/table/tr/td/span/text()').extract()
        l1=[x.replace("  ","").strip() for x in l1 ]
        l1=[x for x in l1 if x ]
        l2=response.xpath('//*[@class="obj-detail"]/div[2]/div[1]/div[2]/table/tr/td/strong/text()').extract()
        l=dict(zip(l2,l1))
        try :
            external_id=response.url.split("=")[-1]
        except:
            pass
        try:
            rent =int(float(l.get("Kaltmiete").replace(" €","")))
        except:
            return
        try:
            bathroom_count = int(float(l.get("Badezimmer")))
        except:
            bathroom_count =1
        try:
            square_meters = int(float(l.get("Wohnfläche").replace(" m²","")))
        except:
            pass
        try:
            if "€" in l.get("Kaution") :
                deposit= int(float(l.get("Kaution").replace(".","").replace(" €","")))
            else:
                deposit = int(float(re.findall(r'\b\d+\b', l.get("Kaution"))[0])) * rent
        except:
            pass
        try:
            room_count = math.ceil(float(l.get("Anzahl Zimmer").replace(",",".")))
        except:
            room_count=1

        try :
            floor=l.get("Etage")
        except:
            pass
        try:
            utilities = int(float(l.get("Nebenkosten").replace(" €","")))
        except:
            pass

        try :
            available=l.get("Bezugsfrei ab")
        except:
            pass
        # extras = "".join( response.xpath('//div[@class="datablock freetext alternate"]/span/span/text()').extract())
        description = "".join(response.xpath('//div[@class="obj-freetext"]/div/span//descendant::text()').extract())
        description=description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        if "pkw-stellplatzdie" in description.lower():
            parking=True
        try:
            active= "".join(response.xpath('//div[@class="datablock freetext"]/span/strong/text()').extract())
            if "ausstattung" in active.lower() :
                furnished = True
        except:
            pass
        images = response.xpath('.//div[@class="fotorama"]/div/@data-img').extract()
        landlord_name="".join(response.xpath('.//div[@class="name"]/strong/text()').extract()).strip()
        if landlord_name =="" :
            landlord_name="Braa-Immobilien"
        landlord_number ="".join(response.xpath('.//div[@class="datablock asp pd clearfix"]/p/span[1]/span/span/text()').extract()).strip()
        if landlord_number == "":
            landlord_number = "+49 (0) 621 545 49 100"
        landlord_email = "info@grundmannimmobilien.de"
        try:
            energy_label=l.get("Energieeffizienzklasse")
        except:
            pass
        address = l.get("PLZ") + ", " +l.get("Ort") + " ,"+ l.get("Land")
        city=l.get("Ort")
        pro= l.get("Objekttyp")
        if "wohnung" in pro.lower() or "wohnung" in title.lower() :
            property_type="apartment"
        elif "haus" in pro.lower() or "haus" in title.lower():
            property_type="house"
        else :
            return
        longitude, latitude = extract_location_from_address(address)
        zipcode, cityy, addres = extract_location_from_coordinates(longitude, latitude)
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
