# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "hauslr"
    start_urls = ['http://www.hauslr.com/rent/']
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
        urls1 = response.xpath('//a[@class="modalLoad"]/@href').extract()
        urls2 = response.xpath('//a[@class="modalLoad"]/@href').extract()
        urls=dict(zip(urls1,urls2))
        for x,y in urls.items():
            url = "http://www.hauslr.com"+x
            yield scrapy.Request(url=url, callback=self.populate_item)
        pages=response.xpath('//*[@id="houses"]/div/div/div/div[2]/ul/li').extract()
        for i in range(2,len(pages)):
            page=f"http://www.hauslr.com/index.asp?BlankCall=Yes&BlankAction=ResponsiveThemeListingsWidgetMoveToPage&ID=561755&Page={i}"
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
        title = "".join(response.xpath('/html/body/div[4]/header/h1/text()').extract()).strip()
        details ="".join(response.xpath("/html/body/div[4]/header/h2/text()").extract()).split("/")
        try:
            rent = int(details[2].replace("$","").replace(",","").strip())
        except:
            return
        try:
            bathroom_count = int(re.findall(r'\b\d+\b',details[1])[0])
        except:
            pass
        try:
            room_count = int(re.findall(r'\b\d+\b',details[0])[0]) +  int(re.findall(r'\b\d+\b',details[0])[1])
        except:
            room_count = int(re.findall(r'\b\d+\b',details[0])[0])
        try:
            external_id = details[3].split("#:")[1]
        except:
            pass



        extras ="".join(response.xpath('//*[@id="property-info"]/div/div[1]/p[1]/text()').extract())
        if "dishwasher" in extras.lower():
            dishwasher=True
        if 'washer' in extras.lower():
            washing_machine=True
        if "park" in extras.lower():
            parking=True
        l=response.xpath('//table[@class="details-table"]/tbody/tr/th/text()').extract()
        if l[5] =='Property Tax':
            l.remove(l[4])
        else :
            square_meters=int(l[5].split("-")[0].replace("< ","").replace("+",""))
        d = [(l[i], l[i + 1]) for i in range(0, len(l), 2)]
        ext=dict(d)
        if "Multiplex" in ext.get("Property Type") :
            property_type="apartment"
        else :
            property_type="house"

        if ext.get("Pool")=="None" :
            pass
        else :
            swimming_pool=True
        if ext.get('Garage Type'):
            parking=True

        description = "".join(response.xpath('//*[@id="property-info"]/div/div[1]/p/text()').extract())
        images = response.xpath('.//ul[@class="property-galery"]/li/a/img/@src').extract()
        landlord_name = "".join(response.xpath('//*[@id="property-info"]/div/div[1]/p[3]/text()').extract()).replace("Listing Office: ","")
        address = response.xpath('/html/body/div[3]/ul/li[2]/span/text()').extract()[0].strip()
        city=address.split(",")[2]
        longitude, latitude = extract_location_from_address(address)
        zipcode, cityy, address = extract_location_from_coordinates(longitude, latitude)
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
        # item_loader.add_value("landlord_phone", landlord_number)  # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
