
# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates
import datetime


class FlexpropertydevelopmentsPyspiderCanadaEnSpider(scrapy.Spider):
    name = "FlexPropertyDevelopments_PySpider_canada_en"
    start_urls = ['https://flexproperty.ca/all-listings/']
    allowed_domains = ["flexproperty.ca"]
    country = 'canada'
    locale = 'en'
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
        urls = response.css(
            "div.cbp-item div.card-img-container a::attr(href)").getall()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        address = response.css(
            "p.agent-info__address.m-0.p-0::text").getall()[1].strip()
        if address == '':
            address = response.css(
                "div.col-md-10.d-flex.flex-column h2::text").get()
        properties = response.css("div.card-body h6::text").getall()
        title = properties[0] + ' ' + properties[1]
        room_count = None
        data = properties[2:]
        bathroom_count = None
        for item in data:
            if item.lower().find("bedrooms") != -1:
                if item.lower().find("bachelor") == -1:
                    room_count = item[0]
                else:
                    room_count = 1
            if item.lower().find("baths") != -1:
                bathroom_count = item[0] 
        property_type = 'apartment'
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)
        amenities = response.css("div.row.mb-0.mb-md-5.px-5 div.col.mt-5 ul li strong::text").getall()
        parking = False
        pets_allowed = False
        furnished = False
        description = None
        available_date = None
        water_cost = False
        heating_cost = False
        washing_machine = False
        monthes = {
            "Jan": "January", "Feb": "February",
            "Mar": "March", "Apr": "April", "May": "May", "Jun": "June", "Jul": "July",
            "Aug": "August", "Sep": "September", "Sept": "September", "Oct": "October",
            "Nov": "November", "Dec": "December"
        }
        for amenity in amenities:
            if amenity.lower().find("pet friendly") != -1:
                pets_allowed = True
            if amenity.lower().find("parking") != -1:
                parking = True
            if amenity.lower().find("heat") != -1:
                heating_cost = True
            if amenity.lower().find("water") != -1:
                water_cost = True
            if amenity.lower().find("laundry") != -1:
                washing_machine = True
            if amenity.lower().find("building description") != -1:
                description = amenity[amenity.find(":") + 2:]
            if amenity.lower().find("furnished") != -1:
                furnished_answer = amenity[amenity.find(":") + 2:]
                if len(furnished_answer) > 1:
                    if furnished_answer[0] == 'u' or furnished_answer[0] == 'U':
                        furnished = True
            if "Availability" in amenity:
                month = amenity[amenity.find(":") + 2:]
                if any(char.isdigit() for char in month):
                    date = month.split(" ")
                    if date[0] in monthes:
                        date[0] = monthes[date[0]]
                    month_data = datetime.datetime.strptime(date[0], "%B")
                    todays_date = datetime.datetime.now()
                    available_date = datetime.datetime(
                        todays_date.year, month_data.month, int(date[1]))
                    available_date = available_date.date()
                elif "immediate" not in month and "Immediate" not in month:
                    month_data = datetime.datetime.strptime(month, "%B")
                    todays_date = datetime.datetime.now()
                    available_date = datetime.datetime(
                        todays_date.year, month_data.month, 1)
                    available_date = available_date.date()
                else:
                    available_date = datetime.datetime.now().date()
        if description == None:
            descriptions = response.css(
                "div.row.mb-0.mb-md-5.px-5 div.col.mt-5 p::text").getall()[1:]
            description = ''
            for des in descriptions:
                description += des
        else:
            descriptions = response.css(
                "div.row.mb-0.mb-md-5.px-5 div.col.mt-5 p::text").getall()[1:]
            description += ' '
            for des in descriptions:
                description += des

        if room_count == None or room_count == "0":
            room_count = 1
        if description.lower().find("furnished") != -1:
            furnished = True
        if description.lower().find("parking") != -1:
            parking = True
        if description.lower().find("laundry") != -1:
            washing_machine = True

        images = response.css(
            "div.row.agent-info__gallery-pics.mt-5 a::attr(href)").getall()
        rent = response.css("h2.text-center.py-3.mx-0.h2bg::text").get()
        landlord_email = response.css("div.row.my-3.px-5 a ::text").get()
        landlord_name = "Flex Property Developments"

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # String => date_format
        item_loader.add_value("washing_machine",washing_machine)
        if available_date:
            item_loader.add_value("available_date", str(available_date))

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("water_cost", water_cost)  # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int
        item_loader.add_value("currency", "CAD")  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String
        if rent == None:
            return

        self.position += 1
        yield item_loader.load_item()
