# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader

from ..helper import *
import math


class panoramicproperties(scrapy.Spider):
    name = "panoramicproperties"
    allowed_domains = ["panoramicproperties.ca"]
    start_urls = ['https://www.panoramicproperties.ca/apartments-for-rent/cities/mississauga'
        , "https://www.panoramicproperties.ca/apartments-for-rent/cities/niagara-falls"
        , "https://www.panoramicproperties.ca/apartments-for-rent/cities/ottawa"
        , "https://www.panoramicproperties.ca/apartments-for-rent/cities/st-catharines"
        , "https://www.panoramicproperties.ca/apartments-for-rent/cities/sudbury"
        , "https://www.panoramicproperties.ca/apartments-for-rent/cities/thorold"
        , "https://www.panoramicproperties.ca/apartments-for-rent/cities/wallaceburg"
        , "https://www.panoramicproperties.ca/apartments-for-rent/cities/welland"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//a[@class="details-button"]/@href').extract()
        for x in range(len(urls)):
            url = "https://www.panoramicproperties.ca/apartments-" + urls[x][1:]
            yield scrapy.Request(url=url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        room_count = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        swimming_pool = None
        square_meters = None
        suites = response.xpath('//div[@class="suite"]').extract()
        for i in range(len(suites)):
            available = response.xpath('//div[@class="suite-avail-data data"]/div/text()').extract()[i]
            if available != "Available":
                pass
            else:
                item_loader = ListingLoader(response=response)
                title = response.xpath('//*[@id="content"]/div/div/div[1]/div[2]/h1/text()').extract()[0].strip()
                address = response.xpath('//*[@id="content"]/div/div/div[1]/div[2]/div[1]/text()').extract()[0].strip()
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
                rent = response.xpath('//div[@class="suite-rent-data data"]/span/text()').extract()[i].strip().replace(
                    "$", "")
                if "N/A" in \
                        "".join(response.xpath('//div[@class="suite-size-data data"]/text()').extract()).split("\n")[
                        1:][i]:
                    pass
                else:
                    square_meters = \
                    response.xpath('//div[@class="suite-size-data data"]//span[@class="value"]/text()').extract()[i]
                bathroom_count = round(float(
                    response.xpath('//div[@class="suite-bath-data data"]//span[@class="value"]/text()').extract()[
                        i].strip()))
                filtered=response.xpath('//div[@class="cms-content"]/p/text()').extract()
                ff=[]
                for f in filtered :
                    if "call" not in f :
                        ff.append(f)
                description = "".join(ff)
                desc = []
                d2 = response.xpath('//div[@class="cms-content"]/ul/li/text()').extract()
                for j in d2:
                    desc.append(j + " --")
                desc = " Building Features include: " + "".join(desc)
                if len(description) > 10:
                    pass
                else:
                    filtered = response.xpath('//div[@class="cms-content"]/div/p/text()').extract()
                    ff=[]
                    for f in filtered:
                        if "call" not in f:
                            f.append(f)
                    description = "".join(filtered)
                description = description + desc

                proptype = response.xpath('//div[@class="suite-type-data data"]/text()').extract()[i].strip()
                if proptype == "Bachelor":
                    property_type = "studio"
                    room_count = 1
                else:
                    property_type = "apartment"
                    rex = re.findall(r'\b\d+\b', proptype)
                    if rex:
                        room_count = rex[0]
                    else:
                        room_count = '1'

                images = response.xpath('//div[@class="gallery-image"]/a/@href').extract()
                landlord_name = "Panoramic Properties Inc."
                landlord_number = response.xpath('//div[@class="phone"]/text()').extract()[0]
                extras = response.xpath('//div[@class="span amenitie 12"]/li/text()').extract()
                extras2 = response.xpath('//div[@class="cms-content"]/ul/li/text()').extract()
                for j in extras:
                    if "dishwasher" in j.lower():
                        dishwasher = True
                    if "balconies" in j.lower():
                        balcony = True
                    if "parking" in j.lower():
                        parking = True
                    if "elevator" in j.lower():
                        elevator = True
                    if "pool" in j.lower():
                        swimming_pool = True
                    if "laundry" in j.lower():
                        washing_machine = True
                for j in extras2:
                    if "dishwasher" in j.lower():
                        dishwasher = True
                    if "balcon" in j.lower():
                        balcony = True
                    if "parking" in j.lower():
                        parking = True
                    if "elevator" in j.lower():
                        elevator = True
                    if "pool" in j.lower():
                        swimming_pool = True
                    if "laundry" in j.lower():
                        washing_machine = True
                # # MetaData
                item_loader.add_value("external_link", response.url + f"#{i}")  # String
                item_loader.add_value("external_source", self.external_source)  # String
                # item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String
                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", str(latitude))  # String
                item_loader.add_value("longitude", str(longitude))  # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type",
                                      property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", 1)  # Int

                # item_loader.add_value("available_date", available)  # String => date_format

                # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                # item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
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
                # item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD")  # String
                #
                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name)  # String
                item_loader.add_value("landlord_phone", landlord_number)  # String
                # item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
