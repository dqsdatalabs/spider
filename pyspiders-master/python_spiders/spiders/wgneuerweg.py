# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class wgneuerweg(scrapy.Spider):
    name = "wgneuerweg"
    start_urls = ['https://www.wg-neuerweg.de/wohnungssuche/?zvon=1&zbis=10&mvon=0&mbis=1225&fvon=0&fbis=150']
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
        urls = response.xpath('//div[@class="immo-infos-bottom-link"]/a/@href').extract()
        for x in range(len(urls)):
            url = "https://www.wg-neuerweg.de/wohnungssuche/"+urls[x]
            # print(url)
            yield scrapy.Request(url=url, callback=self.populate_item)
        # pages=response.xpath('//div[@class="rh_pagination"]/a/@href').extract()[1:]
        # for page in pages :
        #     yield scrapy.Request(url=page, callback=self.parse)


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
        external_id = None
        heating_cost=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('.//div[@class="hyphenate"]/h1/text()').extract()).strip()
        property_type="apartment"
        extras1 = response.xpath('//div[@class="immo-details-table table-informationen"]/div/div/i').extract()
        extras2=response.xpath('//div[@class="immo-details-table table-informationen"]/div/div[@class="immo-details-table-cell cell-left"]/text()').extract()
        extras3=response.xpath('//div[@class="pt-4"]/text()').extract()
        for j in extras3 :
            if "park" in j.lower():
                parking=True
        extras=dict(zip(extras2,extras1))
        for k,v in extras.items() :
            if k == "Balkon:" and "check" in v :
                balcony=True
            if k == "Aufzug:" and "check" in v:
                elevator = True
        data=[]
        d=response.xpath('//div[@class="immo-details-table-cell cell-right"]/text()').extract()
        for i in d :
            if i.strip().replace("\r\n","") == "" :
                pass
            else:
                data.append(i.replace("\r\n","").strip())
        try:
            rent = int(float(data[0].replace(" €", "").replace(",", ".")))
        except:
            return
        try:
            utilities = int(float(data[1].replace(" €", "").replace(",", ".")))
        except:
            pass
        try:
            heating_cost = int(float(data[2].replace(" €", "").replace(",", ".")))
        except:
            pass
        try:
            square_meters = int(float(data[7].replace(" m²", "").replace(",", ".")))
        except:
            pass
        pass
        try:
            external_id = data[4]
        except:
            pass
        try:
            room_count = int(data[6])
            bathroom_count=1
        except:
            pass
        try:
            floor = data[5]
        except:
            pass
        try:
            available = data[8]
        except:
            pass
        try:
            energy_label = data[9]
        except:
            pass

        description = "".join(response.xpath('//div[@class="immo-details-text"]/div[@class="immo-details-area-content"]/text()').extract()[0:-1])
        images = response.xpath('//div[@class="slideshow-image"]/a/@href').extract()
        landlord_name = "".join(response.xpath('//div[@class="text-center font-weight-bold"]/text()').extract())
        landlord_number = "".join(response.xpath('//div[@class="d-table ml-auto mr-auto mb-3"]/div[2]/div[3]/text()').extract())
        landlord_email = "".join(response.xpath('//div[@class="d-table ml-auto mr-auto mb-3"]/div[1]/div[3]/a/text()').extract())
        a1 = response.xpath('//div[@class="inh-abs-titel immo-details-adresse hyphenate"]/h2/text()').extract()[0].strip()
        a2=response.xpath('//div[@class="inh-abs-titel immo-details-adresse hyphenate"]/h2/span/text()').extract()[0].strip()
        address=(a1+a2).replace("|",",")
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

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine)  # Boolean
        # item_loader.add_value("dishwasher", dishwasher)  # Boolean

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
