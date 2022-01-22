# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "montrealliving"
    start_urls = ['https://montrealliving.info/en/properties/our-properties']
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
        urls = response.xpath('//*[@id="jForm"]/div[1]/dl/dt[1]/a/@href').extract()
        for x in urls:
            url = "https://montrealliving.info"+x
            yield scrapy.Request(url=url, callback=self.populate_item)
        pages=response.xpath('//*[@class="pagenav"]/@href').extract()
        for i in pages:
            page= "https://montrealliving.info"+i
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
        square_meters=None
        swimming_pool=None
        external_id = None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//*[@id="component"]/main/div[2]/div[1]/h1/text()').extract()).strip()
        l1=response.xpath('//table[@class="jea-data"]/tr/th/text()').extract()
        l2=response.xpath('//table[@class="jea-data"]/tr/td/text()').extract()
        for x in l2 :
            if x.strip() == "" :
                l2.remove(x)
        property=dict(zip(l1,l2))
        try :
            room_count = property.get("Number of rooms")
            if room_count == None:
                room_count=property.get("Number of bedrooms")
            room_count=room_count.strip()
            room_count=int(room_count)
        except :
            room_count=1
        try :
             if property.get("Property condition").strip() =="Furnished" :
                 furnished=True
        except :
            pass
        try:
            square_meters=int(property.get("Living space").replace(" m²",""))
        except:
            pass

        try:
            bathroom_count = int(property.get("Number of bathrooms").strip())
        except:
            bathroom_count =1
        try:
            rent = property.get("Rent").replace(",","").replace("$","").replace(" ","").strip()
        except:
            return
        description="".join(response.css("div[class='property-description clr'] ::text").getall())
        description=description_cleaner(description)

        images=[]
        imgs = response.xpath('.//a[@class="jea_modal"]/@href').extract()
        for x in imgs:
            images.append(("https://montrealliving.info"+x).replace(" ","%20").strip())
        if "house" in description :
            property_type="house"
        else :
            property_type="apartment"
        address = "".join(response.xpath('//*[@id="component"]/main/div[3]/address/text()').extract()).strip().replace("\n",",").strip()
        external_id="".join(response.xpath('//*[@id="component"]/main/h2/text()').extract()).strip().replace("Ref : ","")
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        details ="".join(response.xpath('//*[@id="component"]/main/div[3]/ul//li/text()').extract()) + " " + description
        if "terrace" in details.lower() :
            terrace=True
        if "washer" in details.lower() :
            washing_machine=True
        if "furnished" in details.lower() :
            furnished=True
        if "parking" in details.lower() :
            parking=True
        if "elevator" in details.lower() :
            elevator=True
        if "pool" in details.lower() :
            swimming_pool=True
        if "balcon" in details.lower() :
            balcony=True
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(
            description,details,item_loader)
        if "basile" in description.lower() :
            landlord_name="Basile"
            landlord_number="514-999-9143"
            landlord_email="basile@montrealliving.info"
        elif "daniel" in description.lower() :
            landlord_name="Daniel"
            landlord_number="514-637-5353"
            landlord_email="lachine@montrealliving.info"
        elif "mynthia" in description.lower() :
            landlord_name="Cynthia"
            landlord_number="514-512-3146"
            landlord_email="duff@montrealliving.info"
        elif "muzanne" in description.lower() :
            landlord_name="Suzanne"
            landlord_number=" 438-403-1128"
            landlord_email=" masson@montrealliving.info"
        elif "mathieu" in description.lower() :
            landlord_name="Mathieu"
            landlord_number="514-683-6666"
            landlord_email="goyer@montrealliving.info"
        else:
            landlord_name="Basile"
            landlord_number="514-999-9143"
            landlord_email="basile@montrealliving.info"
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
        # item_loader.add_value("available_date", available)  # String => date_format
        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
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
