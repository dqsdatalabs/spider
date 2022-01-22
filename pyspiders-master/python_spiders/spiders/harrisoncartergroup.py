# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "harrisoncartergroup"
    start_urls = ['https://www.harrisoncartergroup.com/current-ontario-home-listings/']
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//div[@class="ajax-content-roll-items row"]/div/div/div/h5/a/@href').extract()
        for x in urls:
            url = x
            yield scrapy.Request(url=url, callback=self.populate_item)
        # pages=response.xpath('//*[@id="houses"]/div/div/div/div[2]/ul/li').extract()
        # for i in range(2,len(pages)):
        #     page=f"http://www.hauslr.com/index.asp?BlankCall=Yes&BlankAction=ResponsiveThemeListingsWidgetMoveToPage&ID=561755&Page={i}"
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
        available=None
        pets_allowed=None
        square_meters=None
        swimming_pool=None
        external_id = None
        rent=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//*[@class="col-sm-12"]/div/div/h1/text()').extract()).strip()
        if "RENT" not in title:
            return
        details ="".join(response.xpath('//*[@class="col-sm-12"]/div/div/p/text()').extract())
        if "condo" in details.lower():
            property_type = "apartment"
        else :
            property_type = "house"
        if "garage" in details :
            parking=True
        l=response.xpath('//div[@class="col-sm-7"]/div/div/p/strong/text()').extract()
        l1=response.xpath('//div[@class="col-sm-7"]/div/div/p/text()').extract()
        for x in l1 :
            if "$" in x :
                rent1 = x.replace("$", "").replace(",", "").replace(".00", "").strip()
                rent=re.findall(r'\b\d+\b', rent1)[0]
            if "Bedrooms" in x or "Bedroom" in x :
                room_count = int(re.findall(r'\b\d+\b',x)[0])
            if "Bathrooms" in x or "Bathroom" in x :
                bathroom_count = int(re.findall(r'\b\d+\b', x)[0])
        allstuff=dict(zip(l,l1[0:len(l)]))

        try :
            available = allstuff.get("Available:")
        except:
            available=None
        description="".join(l1[len(l):])
        if  len(description) < 5 :
            description="".join(response.xpath('//div[@class="col-sm-7"]/div/div/p/span/text()').extract())
        extras =description
        if "dishwasher" in extras.lower():
            dishwasher=True
        if 'washer' in extras.lower() or "laundry" in extras.lower():
            washing_machine=True
        if "garage" or "parking" in extras.lower():
            parking=True
        if 'pets' in extras.lower():
            pets_allowed=True
        if 'pool' in extras.lower():
            swimming_pool=True
        for k in response.xpath('//div[@class="col-sm-7"]/div/div/ul/li/text()').extract():
            if "pets" in k.lower():
                pets_allowed = True

        images = response.xpath('//*[@class="container"]/div/div/div/div/div/div/div[1]/ul/li/img/@src').extract()
        for i in range(len(images)):
            images[i]=images[i].replace("-172x113","")
        landlord_name = "HARRISON CARTER GROUP"
        landlord_number="(519) 473-8300"
        landlord_email="info@harrisoncartergroup.com"
        address = title.replace("FOR RENT: ","")
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
        # item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        # item_loader.add_value("elevator", elevator)  # Boolean
        # item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "CAD")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
