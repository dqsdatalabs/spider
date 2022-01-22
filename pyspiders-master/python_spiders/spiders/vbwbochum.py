# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class hermannimmobilien(scrapy.Spider):
    name = "vbwbochum"
    start_urls = ['https://www.vbw-bochum.de/zuhause-finden/mietangebote']
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
        urls= response.xpath('//div[@class="objectcard"]/a/@href').extract()
        for x in range(len(urls)):
            url ="https://www.vbw-bochum.de/" + urls[x]
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
        external_id =None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('.//h1[@class="title"]/text()').extract()).strip()
        l=response.xpath('/html/body/section[2]/div[1]/div/div/div[2]/div[1]/ul/li/span/text()').extract()
        l= [(l[i+1], l[i]) for i in range(0, len(l), 2)]
        l=dict(l)
        l2=response.xpath('/html/body/section[2]/div[2]/div/div/div[1]/div[2]/dl/dd/text()').extract()
        l3=response.xpath('/html/body/section[2]/div[2]/div/div/div[1]/div[2]/dl/dt/text()').extract()
        ll=dict(zip(l3,l2))

        try:
            rent =int(float(ll.get("Kaltmiete").replace(" €","").replace(",00","").replace(",",".")))
        except:
            return
        try:
            external_id = "".join(response.xpath('//*[@class="objnr"]/text()').extract())
        except:
            pass
        bathroom_count = 1
        try:
            square_meters = int(float(l.get("Wohnfläche").replace(" m","").replace(",",".")))
        except:
            pass
        try:
            deposit =  int(float(ll.get("Kaution").replace(" €","").replace(",00","").replace(",",".")))
        except:
            pass
        try:
            room_count = int(float(l.get("Zimmer")))
        except:
            pass
        try:
            heating_cost = int(float(ll.get("Heizkosten").replace(" €","").replace(",00","").replace(",",".")))
        except:
            pass
        try:
            if "Ausstattung" in "".join(response.xpath('/html/body/section[2]/div[2]/div/div/div[2]/h4[1]/text()').extract()) :
                furnished = True
        except:
            pass
        try :
            floor=l.get("Etage")
        except:
            pass
        try:
            utilities = int(float(ll.get("Nebenkosten").replace(" €","").replace(",00","").replace(",",".")))
        except:
            pass

        try :
            available=l.get("Verfügbarkeit")
        except:
            pass
        extras = "".join(response.xpath('//*[@id="main"]/div[3]/div[4]/div/div/ul/li/text()').extract())
        description = response.css("div[class='col-md-6 dataright'] p ::text").getall()
        for x in description:
            if "fragen" in x.lower() or "whatsapp" in x.lower() :
                description.remove(x)
        description="".join(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        images = response.xpath('//ul[@class="slides"]/li/a/@href').extract()
        floor_plan_images=response.xpath('//*[@class="groundplans mb-5"]/img/@src').extract()
        landlord_name="".join(response.xpath('/html/body/section[2]/div[2]/div/div/div[1]/div[4]/p/strong/text()').extract()).strip()
        if landlord_name =="" :
            landlord_name="VBW Bauen und Wohnen GmbH"
        landlord_number ="".join(response.xpath('/html/body/section[2]/div[2]/div/div/div[1]/div[4]/p/text()').extract()).strip().replace("Telefon:","")
        if landlord_number == "":
            landlord_number = "+49 234 310-310"
        landlord_email = "info@vbw-bochum.de"
        address = "".join(response.xpath('//div[@class="address"]/text()').extract())
        if "wohnung" in title.lower() :
            property_type="apartment"
        else:
            property_type="house"
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
