# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import json

import scrapy
from scrapy import Selector
from scrapy.http import HtmlResponse

from ..loaders import ListingLoader
import requests
import re
from ..helper import *
import math


class devimco(scrapy.Spider):
    name = "devimco"
    allowed_domains = ["devimco.com"]
    start_urls = [{"url":'https://devimco.com/appartements/rive-sud/brossard/eolia/eolia-appartements#appartements-disponibles',"code":8}
                  ,{"url":"https://devimco.com/appartements/montreal/griffintown/st-ann/st-ann-appartements#appartements-disponibles","code":5}
                  ,{"url":"https://devimco.com/appartements/montreal/shaughnessy/alexander/alexander-appartements#appartements-disponibles","code":6}
                  ,{"url":"https://devimco.com/appartements/montreal/griffintown/hexagone/l-hexagone#appartements-disponibles","code":3}
                  ,{"url":"https://devimco.com/appartements/rive-sud/brossard/lumeo/lumeo-appartements#appartements-disponibles","code":2}
                  ,{"url":'https://devimco.com/appartements/rive-sud/brossard/nobel/nobel-appartements#appartements-disponibles',"code":9}]

    country = 'canada'  # Fill in the Country's name
    locale = 'fr'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    # 1. SCRAPING level 1
    def start_requests(self):
        for i in range (len(self.start_urls)):
            yield scrapy.Request(url=self.start_urls[i].get("url"), method="GET", callback=self.parse,meta={"code":self.start_urls[i].get("code")})

        # 3. SCRAPING level 3

    def parse(self, response, **kwargs):
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
        swimming_pool = None
        # suites = response.xpath('//div[@class="apartments"]').extract()
        item_loader = ListingLoader(response=response)
        title = response.xpath('//span[@class="main"]/text()').extract()[0]
        address = "".join(response.xpath('//div[@class="location"]/text()').extract()).strip().replace("-",",")
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, adress = extract_location_from_coordinates(longitude, latitude)
        images = []
        imgs = response.xpath('//div[@class="navigation-container"]/ul/li/img/@src').getall()
        for k in imgs:
            images.append("https://devimco.com" + k)
        extras = response.xpath('//div[@class="label"]/text()').extract()
        for j in extras:
            if "vaisselle" in j.lower():
                dishwasher = True
            if "terrasse" in j.lower():
                terrace = True
            # if "parking" in j.lower():
            #     parking = True
            if "ascenseur" in j.lower():
                elevator = True
            if "piscine" in j.lower():
                swimming_pool = True
            if "laveuse" in j.lower():
                washing_machine = True

        landlord_name = "DEVIMCO IMMOBILIER"
        landlord_number = "".join(response.xpath('//*[@id="app"]/div[3]/section[2]/div/div[2]/div/a/text()').getall()).strip()

        # # MetaData

        item_loader.add_value("external_source", self.external_source)  # String
        # item_loader.add_value("external_id", external_id) # String
        # item_loader.add_value("description", description)  # String
        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor) # String


        # item_loader.add_value("available_date", available)  # String => date_format

        # item_loader.add_value("pets_allowed", True)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array


        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        # item_loader.add_value("landlord_email", landlord_email) # String
        head={"referer": response.url }
        yield scrapy.Request(url=f'https://devimco.com/appartements/api/apartment_groups?offset=0&limit=60&type=default&building={response.meta.get("code")}',headers=head,meta={"item_loader":item_loader,"url":response.url,"title":title},callback=self.units,dont_filter=True)


    def units(self,response):
        # print(response.xpath('//div[@class="apartments"]').extract())
        # data = json.loads(response.body)
        room_count=None
        data = json.loads(response.body)["html"]
        sel = Selector(text=data, type="html")
        aps=sel.xpath('//div[@class="apartment"]')
        # print(len(aps))
        for i,ap in enumerate(aps):
            item_loader = response.meta.get("item_loader")
            # print(response.meta.get("title")+f" {i}")
            item_loader.replace_value("title", response.meta.get("title")+f" {i}")  # String
            lst=Selector(text=ap.extract()).xpath('.//ul[@class="list-labels"]/li/text()').extract()
            if lst[0]=="Maison de ville" or lst[0]=="Penthouse":
                property_type="house"
            elif lst[0] == "Studio" :
                property_type="studio"
                room_count=1
            else :
                room_count = int(re.findall(r'\b\d+\b', lst[0])[0]) + 1
                property_type="apartment"
            bathroom_count=int(lst[2].replace(" sdb",""))
            square_meters=int(float(lst[1].replace(" pi²","")))
            rent=int(Selector(text=ap.extract()).xpath('.//div[@class="price"]/p[2]/text()').extract()[0].replace("$/mois",""))
            floor_plan_images=Selector(text=ap.extract()).xpath('.//div[@class="buttons"]/a[1]/@href').extract()
            item_loader.replace_value("external_link", response.meta.get("url") + f"#{i}")  # String
            item_loader.replace_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.replace_value("square_meters", square_meters)  # Int
            item_loader.replace_value("room_count", room_count)  # Int
            item_loader.replace_value("bathroom_count", bathroom_count)  # Int
            # # Monetary Status
            item_loader.replace_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.replace_value("currency", "CAD")  # String
            item_loader.replace_value("position", self.position)  # Int

            self.position += 1
            yield item_loader.load_item()



