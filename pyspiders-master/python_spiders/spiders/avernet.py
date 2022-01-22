# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class avernet(scrapy.Spider):
    name = "avernet"
    allowed_domains = ["avernet.de"]
    start_urls = ['https://www.avernet.de/immobilien/?nutzungsart%5B0%5D=wohnen&vermarktungsart%5B0%5D=miete']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    count=2

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET",meta={'dont_merge_cookies': True}, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//div[@class="card-title"]/h3/a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url,callback=self.populate_item)
        nextpage=f"https://www.avernet.de/immobilien/page/{self.count}/?nutzungsart%5B0%5D=wohnen&vermarktungsart%5B0%5D=miete"
        yield scrapy.Request(url=nextpage, callback=self.parse)

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
        b=response.xpath('//div[@class="detailslisttd"]/div/text()').extract()
        building=[(b[i], b[i + 1]) for i in range(0, len(b), 2)]
        building=dict(building)
        l = response.xpath('//div[@class="infotable d-flex flex-wrap"]/div[@class="half"]/p/text()').extract()
        d = [(l[i], l[i + 1]) for i in range(0, len(l), 2)]
        details=dict(d)
        if building.get("Objektart") == "Wohnung" or "Haus" :
            # external_id = re.findall(r'\b\d+\b', response.url)[0]
            item_loader = ListingLoader(response=response)
            title = response.xpath('.//div[@class="col-12"]/h1/text()').extract()
            try:
                square_meters = int(float(details.get("Quadratmeter").replace(" m²", "").replace(",",".")))
            except:
                pass
            try:
                room_count = int(float(details.get("Zimmer").replace(",",".")))
            except:
                pass
            try:
                rent = int(details.get("Preis").replace(" €", "").replace(".",""))
            except:
                pass
            try:
                external_id = details.get("Kennung")
            except:
                pass
            try:
                zipcode = building.get("PLZ")
            except:
                pass
            try:
                p = building.get("Objektart")
                if p=="Wohnung":
                    property_type="apartment"
                else :
                    property_type="house"
            except:
                pass
            try:
                energy_label = building.get("Energieeffizienzklasse")
            except:
                pass
            try:
                bathroom_count = building.get("Anzahl Badezimmer")
            except:
                pass
            try:
                city = building.get("Ort")
            except:
                pass
            desc1=[]
            desc=response.xpath('//div[@class="tab-beschreibung"]/text()').extract()
            for j in desc:
                if "https" not in j :
                    desc1.append(j)
            description = "".join(desc1).strip()
            if "balkon" in description.lower():
                balcony=True
            if "stellplatz" in description.lower():
                parking=True
            if "waschmaschin" in description.lower():
                washing_machine=True
            images = response.xpath('//*[@id="main"]/div[1]/div/div/a/@href').extract()
            landlord_name = "AVERNET IMMOBILIEN"
            landlord_number = "+49 (0)7621–1675870"
            floor_plan_images= response.xpath('//div[@class="tab-grundriss"]/div/div/a/img/@src').extract()
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
            # item_loader.add_value("address", address)  # String
            # item_loader.add_value("latitude", latitude)  # String
            # item_loader.add_value("longitude", longitude)  # String
            item_loader.add_value("floor", floor) # String
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
            # item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

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
            #
            # # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            # # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()

