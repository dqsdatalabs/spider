# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class WellgroundedrealestateSpider(scrapy.Spider):
    name = "casafoggia"
    allowed_domains = ["casafoggia.net"]
    start_urls = ['https://www.casafoggia.net/affitto/case/']
    country = 'italy'  # Fill in the Country's name
    locale = 'it'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        head = {
            "User-Agent": 'tutorial (+http://www.yourdomain.com)'}
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET",meta={'dont_merge_cookies': True},headers=head, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls1 = response.xpath('//a[@class="lnkhp"]/@href').extract()
        urls2= response.xpath('//a[@class="lnkhp2"]/@href').extract()
        head = {
            "User-Agent": 'tutorial (+http://www.yourdomain.com)'}
        urls=urls1+urls2
        for x in range(len(urls)):
            url = urls[x]
            # print(url)
            yield scrapy.Request(url=url, headers=head,callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        room_count=None
        bathroom_count=None
        floor=None
        parking=None
        elevator=None
        balcony=None
        washing_machine=None
        dishwasher=None
        utilities=None
        terrace=None
        furnished=None
        property_type=None
        energy_label=None
        external_id = response.xpath('//*[@id="contatta"]/form/ul/li[1]/text()[1]').extract()
        if  external_id:
            external_id = response.xpath('//*[@id="contatta"]/form/ul/li[1]/text()[1]').extract()[0].replace("riferimento: ", "")
        item_loader = ListingLoader(response=response)
        title = response.xpath('.//div[@id]/h1/text()[1]').extract()
        details = response.xpath('//*[@id="dati"]/ul/li/text()').extract()
        for x in range (len(details)) :
            if "Appartamento" in details[x] :
                property_type = "apartment"
            elif "Attico" in details[x] :
                property_type = "house"
            if "Piano" in details[x] :
                floor =details[x+1]
        park = response.xpath('//*[@id="accessori"]/ul/li/text()').extract()
        for i in range(len(park)):
            if "Posto auto" in park[i] :
                if "Si" in park[i+1] :
                    parking=True
        building=response.xpath('//*[@id="parti"]/ul/li/text()').extract()
        for j in range(len(building)):
            if "Ascensore" in building[j] and "Si" in building[j+1]:
                elevator=True
            if "Spese condominio" in building[j] and building[j+1] != "0":
                utilities=building[j+1]
        rent = response.xpath('//span[@class="aranciogrande"]/text()').extract()[0]
        square_meters = response.xpath('//span[@class="aranciogrande"]/text()').extract()[1]
        count ="".join(response.xpath('//*[@id="comp1"]//text()').extract()).strip().replace("\r\n\t\t\t\t\t\t\t\t\t","").split("  ")
        for i in range(len(count)) :
            if 'Camere da letto' in count[i]:
                room_count=int(count[i+1])
            if 'Bagni' in count[i]:
                bathroom_count=int(count[i+1])
        extras ="".join(response.xpath('//*[@id="comp2"]//text()').extract()).strip().replace("\r\n\t\t\t\t\t\t\t\t\t","").split("  ")
        for j in range(len(extras)):
            if "Arredato" in extras[j] and "Si" in extras[j+1]:
                furnished = True
            if 'Balconi / veranda' in extras[j] and extras[j] != "0" :
                balcony = True
            if "Terrazzi" in extras[j] and "Si" in extras[j+1]:
                terrace = True
        description = "".join(response.xpath('//*[@id="descr"]/h2/text()').extract())
        if description :
            description = "".join(response.xpath('//*[@id="descr"]/h2/text()').extract())
        images = response.xpath('//*[@id="thumb"]/a/@href').extract()
        landlord_name = "OMNIBUS IMMOBILIARE"
        landlord_number = "0881.617004"
        energ=response.xpath('//*[@id="dati"]/ul/li[2]/img/@src').extract()[0]
        for x in energ :
            if x.isupper():
                energy_label=x
        city="FOGGIA"
        zone=response.xpath('//*[@id="wrap"]/div/h1/span/text()').extract()
        address=zone[0].replace("-",",")+f", {city} "
        longitude,latitude=extract_location_from_address(address)
        zipcode, city, address=extract_location_from_coordinates(longitude,latitude)


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
        item_loader.add_value("latitude", str(latitude) ) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type",property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available)  # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine)  # Boolean
        # item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()







