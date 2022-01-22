# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class Immoblu(scrapy.Spider):
    name = "immoblu"
    allowed_domains = ["immoblu.de"]
    start_urls = ['https://www.immoblu.de']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET",meta={'dont_merge_cookies': True}, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//*[@id="immobilien-mieten"]/div/div/a/@href').extract()
        for x in range(len(urls)):
            url = "https://www.immoblu.de/"+urls[x]
            yield scrapy.Request(url=url,callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        # print(response.body)
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
        external_id =response.url.split("=")[1]
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('.//h1/text()').extract())
        if "Reserviert" in title :
            yield
        else :
            details1 = response.xpath('/html/body/div[5]/div/div[1]/div[3]/table/tr/td/span/text()').extract()
            details2 = response.xpath('/html/body/div[5]/div/div[1]/div[3]/table/tr/td/strong/text()').extract()
            for i in details1 :
                if "\n" in i :
                    details1.remove(i)
            details=dict(zip(details2,details1))
            if "Wohnung" in details.get("Objektart"):
                if "ja" in details.get("Balkon"):
                    balcony=True
                property_type="apartment"
                zipcode=details.get("PLZ")
                address=details.get("Ort").replace("/",",")
                city=details.get("Land")
                square_meters=int(details.get("Wohnfläche").replace(" m²",""))
                room_count=details.get("Anzahl Schlafzimmer")
                bathroom_count=details.get("Anzahl Badezimmer")
                city=details.get("Land")
                if "Stellplätze" in details2 :
                    parking=True
                if "Balkon/Terrasse Fläche" in details2:
                    terrace=True
                rent=int(details.get("Kaltmiete").replace("€",""))
                utilities=int(details.get("Nebenkosten").replace("€",""))
                deposit=3*rent
                if "Ja" in details.get("Wasch/Trockenraum"):
                    washing_machine=True
                description = "".join(response.xpath('//div[@class="objectdetails-freitexte"]/span/span/p/text()').extract())
                titles=response.xpath('//div[@class="objectdetails-freitexte"]/span/strong/text()').extract()
                if "Ausstattung:"  in titles :
                    furnished=True
                images = response.xpath('//div[@class="fotorama"]/div/@data-img').extract()
                landlord_name = "".join(response.xpath('/html/body/div[5]/div/div[2]/div[1]/div[2]/p[1]/strong/text()').extract())
                landlord_number = "".join(response.xpath('/html/body/div[5]/div/div[2]/div[1]/div[2]/p[3]/span[1]/span/text()').extract())
                landlord_email="".join(response.xpath('/html/body/div[5]/div/div[2]/div[1]/div[2]/p[4]/span/span/a/text()').extract())
                address = address +", "+city
                longitude, latitude = extract_location_from_address(address)
                zippcode, city, address=extract_location_from_coordinates(longitude,latitude)

                # # # MetaData
                item_loader.add_value("external_link", response.url)  # String
                item_loader.add_value("external_source", self.external_source)  # String
                item_loader.add_value("external_id", external_id)  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String
                # # # Property Details
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
                #
                # # item_loader.add_value("available_date", available)  # String => date_format
                #
                # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                # item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                item_loader.add_value("terrace", terrace)  # Boolean
                # # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                # # item_loader.add_value("dishwasher", dishwasher)  # Boolean
                #
                # # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                # # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array
                #
                # # # Monetary Status
                item_loader.add_value("rent", int(rent))  # Int
                item_loader.add_value("deposit", deposit) # Int
                # # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "EUR")  # String
                # #
                # # item_loader.add_value("water_cost", water_cost) # Int
                # # item_loader.add_value("heating_cost", heating_cost) # Int
                #
                # item_loader.add_value("energy_label", energy_label)  # String
                #
                # # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name)  # String
                item_loader.add_value("landlord_phone", landlord_number)  # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()












