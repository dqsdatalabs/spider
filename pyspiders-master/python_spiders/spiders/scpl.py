# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class scpl(scrapy.Spider):
    name = "scpl"
    start_urls = ['https://www.scpl.com/residential-rental']
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):

        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET",meta={'dont_merge_cookies': True}, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//ul[@class="cities"]/li/a/@href').extract()
        for x in range(len(urls)):
            url = "https://www.scpl.com"+urls[x]
            if "cities" in url :
                yield scrapy.Request(url=url, callback=self.cities)
            else :
                yield scrapy.Request(url=url, callback=self.populate_item)
    def cities(self,response):
        urls =  response.xpath('//section[@class="properties-list"]/a/@href').extract()
        for url in urls :
            url="https://www.scpl.com"+url
            yield scrapy.Request(url=url, callback=self.populate_item)

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
        square_meters=None
        loop=response.xpath('//span[@class="rent"]/text()[1]').extract()
        for i in range(len(loop)):
            item_loader = ListingLoader(response=response)
            title = response.xpath('//*[@id="content"]/div/section/div[1]/h1/text()').extract()[0].strip()
            building=response.xpath('//ul[@class="amenities"]/li/text()').extract()
            for j in range(len(building)):
                if "parking" in building[j].lower():
                    parking=True
                if "balcon" in building[j].lower():
                    balcony = True
                if "dishwasher" in building[j].lower():
                    dishwasher = True
                if "laundry" in building[j].lower():
                    washing_machine = True
            rent = response.xpath('//span[@class="rent"]/text()[1]').extract()[i].strip().replace("$","").replace(",","")
            if "Call" in rent :
                continue
            else :
                rent=int(rent)
            try:
                square_meters = int(response.xpath('//div[@class="suite-row sqft"]/span/text()[1]').extract()[i].strip())
            except :
                pass
            r_count=response.xpath('//div[@class="suite-row name"]/span/text()[1]').extract()[i].strip()
            if r_count =="Bachelor" :
                room_count=1
            else :
                room_count=int(re.findall(r'\b\d+\b',r_count)[0])
            bathroom_count = round(float(response.xpath('//div[@class="suite-row bath"]/span/text()[1]').extract()[i].strip()))
            description = "".join(response.xpath('//div[@class="cms-content"]/p/text()').extract())
            if "suite" in description :
                property_type = "house"
            else:
                property_type="apartment"
            images = response.xpath('//*[@id="content"]/div/section/div[1]/section[1]/section/section/div/a/img/@src').extract()
            landlord_name = "".join(response.xpath('//*[@id="content"]/div/section/div[1]/section[2]/div/div[1]/div[3]/text()[2]').extract()).strip()
            if not landlord_name :
                landlord_name="Shelter Canadian Properties Limited"
            landlord_number = "".join(response.xpath('//*[@id="content"]/div/section/div[1]/section[2]/div/div[1]/div[5]/a[1]/text()').extract()).strip()
            if not landlord_number :
                landlord_number= "(204) 475-9090"
            landlord_email="".join(response.xpath('//*[@id="content"]/div/section/div[1]/section[2]/div/div[1]/div[5]/a[2]/text()').extract()).strip()
            if not landlord_email :
                landlord_email="shelter@scpl.com"
            address="".join(response.xpath('//div[@class="info address"]/p/text()').extract())
            try :
                longitude,latitude=extract_location_from_address(address)
                zipcode, city, adress=extract_location_from_coordinates(longitude,latitude)
            except:
                ad=address.split(",")
                city=ad[1]
                zipcode=ad[1][-1:-6]
                pass
            floor_plan_images =None
            try :
                floor_plan_images=response.xpath('//li[@class=" print-only"]/img/@src').extract()
            except:
                pass
            # # MetaData
            item_loader.add_value("external_link", response.url+f"#{i}")  # String
            item_loader.add_value("external_source", self.external_source)  # String
            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String
            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type",property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # item_loader.add_value("available_date", available)  # String => date_format

            # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String
            #
            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()







