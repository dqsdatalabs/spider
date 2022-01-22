# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import math

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class gibbesch(scrapy.Spider):
    name = "robertcspies"
    start_urls = ["https://robertcspies.de/wohnen/objekte?type=rent&livingSpace_min=&livingSpace_max=&price_min=&price_max="]
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
        urls= response.xpath('//a[@class="exposeList__item__content__linkedHeadline"]/@href').extract()
        for x in range(len(urls)):
            url = "https://robertcspies.de/"+ urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item,dont_filter=True)
        pages=response.xpath('//li[@class="pagination__list__item"]/a/@href').extract()
        for x in pages:
            yield scrapy.Request(url="https://robertcspies.de/"+x, callback=self.parse)


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
        title = "".join(response.xpath('//h1[@class="exposeData__infos__title"]/text()').extract()).strip()
        l1=response.xpath('.//div[@class="label"]/text()').extract()
        try :
            external_id=response.xpath('.//div[@class="exposeData__infos__number"]/text()').extract()[0].replace("Objektnummer ","").strip()
        except:
            pass
        if "reserviert" in "".join(response.xpath('.//span[@class="status"]/text()').extract()).lower():
            return
        try:
            rent =int(float(response.xpath('.//div[@class="field preise-nettokaltmiete"]/div[@class="value-number"]/text()').extract()[0].replace(".","").replace(",",".").replace(" EUR","")))
        except:
            return
        try:
            bathroom_count = int(float(response.xpath('.//div[@class="field flaechen-anzahl_badezimmer"]/div[@class="value-number"]/text()').extract()[0]))
        except:
            bathroom_count =1
        try:
            square_meters = int(float(response.xpath('.//div[@class="field flaechen-wohnflaeche"]/div[@class="value-number"]/text()').extract()[0].replace("ca. ","").replace(",",".").replace(" m²","")))
        except:
            pass
        try:
            deposit= int(float(response.xpath('.//div[@class="field preise-kaution"]/div[@class="value-number"]/text()').extract()[0].replace(".","").replace(",",".").replace(" EUR","")))

        except:
            pass
        try:
            room_count = math.ceil(float(response.xpath('.//div[@class="field flaechen-anzahl_zimmer"]/div[@class="value-number"]/text()').extract()[0]))
        except:
            room_count=1

        try :
            floor=response.xpath('.//div[@class="field geo-anzahl_etagen"]/div[@class="value-number"]/text()').extract()[0]
        except:
            pass
        try:
            utilities = int(float(response.xpath('.//div[@class="field preise-nebenkosten"]/div[@class="value-number"]/text()').extract()[0].replace(".","").replace(",",".").replace(" EUR","")))
        except:
            pass

        try :
            available=response.xpath('.//div[@class="field verwaltung_objekt-verfuegbar_ab"]/div[@class="value-text"]/text()').extract()[0]
        except:
            pass
        extras = "".join(response.xpath('.//div[@class="label"]/text()').extract()).strip()
        description = "".join(response.css("div[class='exposeDescription__text'] p ::text").getall()).strip()
        description=description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,extras,item_loader)
        if "Anzahl Terrassen" in l1:
            terrace = True
        if "Stellplatz" in l1:
            parking = True
        if "Personenfahrstuhl" in l1 :
            elevator=True
        images=[]
        pics = response.xpath('.//div[@class="exposeGallery__slider"]/div/img/@src').extract()
        for x in pics :
            images.append("https://robertcspies.de/"+x)
        landlord_name="".join(response.xpath('.//div[@class="name"]/text()').extract()).strip()
        if landlord_name =="" :
            landlord_name="Robert C. Spies"
        arr=response.xpath('.//div[@class="links"]/a/@data-tooltip').extract()
        try :
            landlord_number ="".join(arr[0]).strip()
        except:
            landlord_number = "0421-17393-0"
        try:
            landlord_email = "".join(arr[1]).strip()
        except:
            landlord_email = "info@robertcspies.de"
        try:
            energy_label=response.xpath('.//div[@class="field effizienzklasse"]/div[@class="value"]/text()').extract()[0]
        except:
            pass
        pro= title
        if "wohnung" in pro.lower() :
            property_type="apartment"
        elif "haus" in pro.lower() :
            property_type="house"
        else:
            property_type = "apartment"

        # longitude, latitude = extract_location_from_address(address)
        # zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String
        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String
        # # Property Details
        # item_loader.add_value("city", city)  # String
        # item_loader.add_value("zipcode", zipcode)  # String
        # item_loader.add_value("address", address)  # String
        # item_loader.add_value("latitude", str(latitude))  # String
        # item_loader.add_value("longitude", str(longitude))  # String
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
