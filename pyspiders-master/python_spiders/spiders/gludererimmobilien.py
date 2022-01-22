# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class gludererimmobilien(scrapy.Spider):
    name = "gludererimmobilien"
    start_urls = ['https://430459.flowfact-sites.net/immoframe/?country=Deutschland&typefilter=1AB70647-4B47-41E2-9571-CA1CA16E0308%7C0',
                  "https://430459.flowfact-sites.net/immoframe/?country=Deutschland&typefilter=E4DE337C-2DE8-4560-9D5F-1C33A96037B6%7C0"]
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
        urls= response.xpath('//div[@class="grid3 fr"]/h3/a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)
        page=response.xpath('//*[@id="pagination"]/a/@href').extract()
        page=dict(zip(page,page))
        try :
            m = int(max(page.values()).split("pageno=")[1])
            for i in range(2, m):
                urlpage = f"https://430459.flowfact-sites.net/immoframe/?country=Deutschland&typefilter=1AB70647-4B47-41E2-9571-CA1CA16E0308%7C0&&pageno={i}"
                yield scrapy.Request(url=urlpage, callback=self.parse)
        except :
            pass


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
        zipcode=None
        city=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('//*[@id="estate_information"]/article[1]/div[2]/table/tr/td/span/text()').extract()).strip()
        l2=response.xpath('//*[@id="estate_information"]/article[1]/div[2]/table/tr/td/text()').extract()
        for x in l2 :
            if x==' ' :
                l2.remove(x)

        try:
            rent =int(float(l2[l2.index('Nettokaltmiete')+1].replace(" €","").replace(",",".")))
        except:
            try :
                rent =int(float(l2[l2.index('Miete zzgl. NK')+1].replace(" €","").replace(",",".")))
            except:
                return

        external_id=l2[l2.index('Kennung')+1].strip()

        try:
            bathroom_count = int(float(l2[l2.index('Badezimmer')+1].replace(",",".")))
        except:
            bathroom_count=1
        try:
            square_meters = int(float(l2[l2.index('Wohnfläche')+1].replace(",",".").replace(" m²","").replace("ca. ","")))
        except:
            pass
        try:
            deposit =  int(float(l2[l2.index('Kaution')+1].replace(" €","").replace(".","").replace(",",".")))
        except:
            pass
        try:
            room_count = int(float(l2[l2.index('Zimmer')+1].replace(",",".")))
        except:
            pass
        try:
            heating_cost = int(float(l2[l2.index('Warmmiete')+1].replace(" €","").replace(",","."))) -rent
        except:
            pass

        try :
            floor=l2[l2.index('Etage')+1]
        except:
            pass
        try:
            utilities = int(float(l2[l2.index('Nebenkosten') + 1].replace(" €", "").replace(",", ".")))
        except:
            pass
        try :
            available=l2[l2.index('Verfügbar ab')+1]
        except:
            pass
        try:
            pos= l2[l2.index('Lage') + 1].split(" ")
            zipcode=pos[0]
            city=pos[1]
        except:
            pass
        extras = "".join(response.xpath('//div[@class="property-features"]/ul/li/text()').extract())
        try:
            if l2[l2.index('Balkon/Terrasse') + 1] == "ja" :
                balcony=True
                terrace=True
        except:
            pass
        description = response.xpath('//div[@class="pl1 pt2 pb1"]/text()').extract()
        for x in description:
            if "der oben" in x.lower() :
                description.remove(x)
        description="".join(description)
        description=description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        try:
            if "ausstattung" in "".join(response.xpath('//div[@class="pl1 bb "]/h1/text()').extract()).lower() :
                furnished = True
        except:
            pass
        images=[]
        pics=response.xpath('//div[@class=" grid1 fl"]/div/a/@href').extract()
        for y in pics :
            y="http:"+y.replace(" ","")
            images.append(y)
        landlord_name="".join(response.xpath('//*[@id="estate_contact_person"]/div[2]/div[2]/div/strong/text()').extract())
        if landlord_name=="" :
            landlord_name="Dirk Gluderer Immobilien e.K."
        try :
            landlord_number = "".join(response.xpath('//*[@id="estate_contact_person"]/div[2]/div[2]/div/text()').extract()).split("+")[1]
        except:
            landlord_number="+49 (0) 4106 / 3006"
        landlord_email = "".join(response.xpath('//*[@id="estate_contact_person"]/div[2]/div[2]/div/a/@href').extract()).replace("mailto:","")
        if landlord_email=="":
            landlord_email="info@gluderer-immobilien.de"
        pro=l2[l2.index('Objektart')+1]
        if "wohnung" in pro.lower() :
            property_type="apartment"
        else:
            property_type="house"
        address=city + zipcode
        longitude, latitude = extract_location_from_address(address)
        zippcode, city, address = extract_location_from_coordinates(longitude, latitude)

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
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

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
