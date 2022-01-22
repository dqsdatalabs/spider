# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import math

import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class wbgwippertal(scrapy.Spider):
    name = "wbgwippertal"
    start_urls = ["https://www.wbg-wippertal.de/suche.html?REQUEST_TOKEN=6d6b4e9fc5956a924ef6cfc46bd34f89&flat_usenr=Wohnung&flat_district=&flat_space_from=&flat_space_to="]
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
        dataref= response.xpath('//span[@class="button bold detailsbutton"]/@data-referer').extract()
        dataid=response.xpath('//span[@class="button bold detailsbutton"]/@data-id').extract()
        for x in range(len(dataid)):
            url = f"https://www.wbg-wippertal.de/details.html?id={dataid[x]}&referer={dataref[x]}"
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
        pets_allowed=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//*[@id="wohnungsfinder"]/div/div[1]/div[1]/p[1]/text()').extract()).strip()
        l1=response.xpath('.//span[@class="details_label"]/text()').extract()
        for x in l1 :
            if "\xa0" in x or "\r\n" in x :
                l1.remove(x)
        l2=response.xpath('.//span[@class="details_content"]/text()').extract()
        l=dict(zip(l1,l2))

        try:
            rent =int(float(response.xpath('.//p[@class="big bold"]/text()').extract()[0].replace(".","").replace(",",".").replace(" €","")))
        except:
            return

        bathroom_count =1
        try:
            square_meters = int(float(response.xpath('.//p[@class="big bold"]/text()').extract()[2].replace("ca. ","").replace(",",".").replace(" m","")))
        except:
            pass
        try:
            room_count = math.ceil(float(response.xpath('.//p[@class="big bold"]/text()').extract()[1]))
        except:
            room_count=1

        try :
            available=l.get("Frei ab")
        except:
            pass
        try:
            utilities = int(float(l.get("Nebenkostenvoraus. ca.").replace(",",".").replace(" €","")))
        except:
            pass
        try:
            deposit = int(float(l.get("Kaution").replace(",", ".").replace(" €", "")))
        except:
            pass
        try:
            heating_cost = int(float(l.get("Heizkosten ca.").replace(",", ".").replace(" €", "").strip()))
        except:
            pass
        extras="".join(response.xpath('.//ul[@class="details_content"]/li/text()').extract())
        description = "".join(response.xpath('.//p[@class="description"]//descendant::text()').extract()).replace("\r\n\r\n"," ").replace("\r\n"," ")
        description=description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,extras,item_loader)
        if "Kaltmiete Stellplatz:" in l2 :
            parking=True
        images = response.xpath('.//figure[@class="image_container"]/a/@href').extract()
        floor_plan_images=response.xpath('.//figure[@class="plot_image"]/a/@href').extract()
        landlord_name="".join(response.xpath('.//div[@class="ansprechpartner first"]//div[@class="info"]/h3/text()').extract())
        landlord_number = "".join(response.xpath('.//div[@class="ansprechpartner first"]//div[@class="info"]/p/text()').extract()).strip()
        landlord_email = "".join(response.xpath('.//div[@class="ansprechpartner first"]//div[@class="info"]/p/a/text()').extract())
        pro= title
        if "wohnung" in pro.lower() :
            property_type="apartment"
        elif "haus" in pro.lower() :
            property_type="house"
        else:
            property_type = "apartment"
        address="".join(response.xpath('.//p[@class="address"]/text()').extract())
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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
