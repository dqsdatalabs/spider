# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class grtinc(scrapy.Spider):
    name = "grtinc"
    start_urls = ['https://www.grtinc.ca/appartements/']
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls1 = response.xpath('//a[@class="btn btn-transparent product-btn"]/@href').extract()
        url2=response.xpath('//a[@class="btn btn-transparent product-btn"]/@href').extract()
        urls=dict(zip(urls1,url2))
        for x,y in urls.items():
            url = "https://www.grtinc.ca/"+urls[x]
            yield scrapy.Request(url=url, callback=self.ajax)

    # 3. SCRAPING level 3
    def ajax(self, response):
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
        square_meters = None
        swimming_pool = None
        external_id = None
        pets_allowed = None
        heating_cost = None
        item_loader = ListingLoader(response=response)
        ajax = "".join(response.xpath('//div[@class="ajaxcontent"]/@data-ajax').extract())
        url=response.url+f"?type=75&tx_tmgrt_unite%5Bcomplexe%5D={ajax}"
        try :
            address = response.xpath('//address[@class="visible-xs-block"]/text()').extract()[1]
        except :
            address = "".join(response.xpath('//address[@class="visible-xs-block"]/text()').extract())
        title="".join(response.xpath('//h3[@class="section-title"]/text()').extract())

        description="".join(response.css('div[class="section-content-inner"] ::text').getall()).replace("  ","").replace("\n\n\n\n\n"," ").replace('\r\n\n'," ")

        images=[]
        pics = response.xpath('//*[@class="slide"]/@style').extract()
        for x in pics :
            images.append("https://www.grtinc.ca/"+x.replace("background-image: url(","").replace(");",""))

        landlord_name ="Gestion Rochefort et Tessier inc"
        landlord_number ="418-653-1242"
        landlord_email="services@grtinc.ca"
        property_type="apartment"
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        if "piscine" in description.lower():
            swimming_pool = True
        if "buanderie" in description.lower():
            washing_machine = True
        if "animaux acceptés" in description.lower():
            pets_allowed = True
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude",str(longitude))  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email) # String
        yield scrapy.Request(url=url, callback=self.populate_item,meta={"item_loader":item_loader,"url":response.url,"title":title})

    def populate_item(self,response):
        room_count = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        square_meters=None
        available=None
        suites = response.xpath('//strong[@class="table-text"]/text()').extract()
        title1=response.meta.get("title")
        for i in range(len(suites)):
            item_loader = response.meta.get("item_loader")
            title2 = response.xpath('//div[@class="unite-left-wrapper"]//div[@class="top"]/p/text()').extract()[0]
            title=response.meta.get("title") + " " + title2
            try :
                room_count=int(title2.replace("½",""))+1
            except :
                room_count=1
            rent = response.xpath('//strong[@class="table-text"]/text()').extract()[i].replace("$","")
            try :
                square_meters = "".join(response.xpath('//div[@class="unite-middle-wrapper"]//div[@class="bot"]/span[2]/text()').extract()).strip().split("pi")[i]
            except :
                pass
            try :
                available = response.xpath('//div[@class="unite-right-wrapper"]//div[@class="top"]/strong/text()').extract()[i]
            except :
                pass
            try:
                floor_plan_images = "https://www.grtinc.ca/" + response.xpath('//div[@class="popup-entry"]/img/@src').extract()[i]
            except:
                floor_plan_images = []
            if "laveuse" in "".join(response.xpath('//span[@class="show-for-large"]/text()').extract()).lower():
                washing_machine=True
            if "vaisselle" in "".join(response.xpath('//span[@class="show-for-large"]/text()').extract()).lower():
                dishwasher=True


            # # MetaData
            item_loader.replace_value("external_link", response.meta.get("url")+f"#{i}")  # String
            item_loader.replace_value("position", self.position)  # Int
            item_loader.replace_value("external_source", self.external_source)  # String
            # item_loader.add_value("external_id", external_id) # String
            item_loader.replace_value("title", title)  # String
            # item_loader.add_value("description", description)  # String
            item_loader.replace_value("washing_machine", washing_machine)
            item_loader.replace_value("dishwasher", dishwasher)
            # # Property Details
            item_loader.replace_value("square_meters", square_meters)  # Int
            item_loader.replace_value("room_count", room_count)  # Int
            item_loader.replace_value("bathroom_count", 1)  # Int
            item_loader.replace_value("available_date", available)  # String => date_format
            item_loader.replace_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.replace_value("rent", int(rent))  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.replace_value("currency", "CAD")  # String
            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int
            # item_loader.add_value("energy_label", energy_label) # String
            self.position += 1
            yield item_loader.load_item()




