# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from urllib.parse import urlparse, urlunparse, parse_qs
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class GreatapartmentSpider(scrapy.Spider):

    name = "greatapartments"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://greatapartments.ca/'
        yield Request(url, dont_filter=True, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response):
        city_links = response.xpath(
            './/a[contains(@href, "greatapartments.ca/mha_property")]/@href').extract()
        for link in city_links:
            yield scrapy.Request(link, callback=self.populate_item)

    def populate_item(self, response):
        description = description_cleaner("".join(response.css(
            ".mha-details:nth-child(1) .col::text").getall()))
        amenity = " ".join(response.css(".row .mb-5 *::text").getall()) + \
            "".join(response.css(".col li::text").getall())
        amenity = amenity.lower()
        if 'no pet' in amenity:
            amenity = amenity.replace('no pet', 'empty')

        address = response.xpath('.//div[@class="elementor-text-editor elementor-clearfix"]/text()').extract()[1:4]
        for i in range(len(address)):
            address[i] = address[i].strip()
        
        longitude, latitude = extract_location_from_address(''.join(address[0:2]))
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        landlord_phone = response.css(
            ".elementor-row .elementor-button-wrapper a .elementor-button-text::text").getall()[0]
        if not landlord_phone:
            landlord_phone = '825.251.2461'

        images = response.css(".swiper-slide-inner img::attr(src)").getall()
        images = [x for x in images if 'data:image' not in x]
        title = response.url.split('/')[-1]

        apartments = response.css(".col .mha-available-suites")
        j = 1
        for apartment in apartments:
            room_count = '1'
            bathroom_count = None
            square_meters = 0
            deposit = 0
            rent = 0
            rex = ("".join(apartment.css(
                ".wrapper .mb-3:contains(Bed)::text").getall())).lower().replace(' ', '')
            if rex:
                try:
                    room_count = re.search(r'\d+bed', rex)[0].replace('bed','')
                    bathroom_count = re.search(r'\d+bath', rex)[0].replace('bath','')
                except:
                    if room_count == None:
                        room_count = '1'
                    if bathroom_count == None:
                        bathroom_count = '1'

            rex = ("".join(apartment.css(
                ".wrapper .mb-3:contains(q)::text").getall())).lower().replace(' ', '')
            if rex:
                for i in re.findall(r'\d+', rex.replace(',', '')):
                    square_meters += int(i)
                square_meters = int(square_meters/2)

            rex = ("".join(apartment.css(
                ".wrapper .mb-3:contains(posit)::text").getall())).lower().replace(' ', '')
            if rex:
                for i in re.findall(r'\d+', rex.replace(',', '')):
                    deposit += int(i)
                deposit = int(deposit/2)

            rex = ("".join(apartment.css(
                ".wrapper .mb-3:contains(mo)::text").getall())).lower().replace(' ', '')
            if rex:
                for i in re.findall(r'\d+', rex.replace(',', '')):
                    rent += int(i)
                rent = int(rent/2)

            if deposit == None:
                deposit = int(rent/2)

            floor_plan_images = apartment.css(
                ".carousel-inner img::attr(src)").getall()
            floor_plan_images = [
                x for x in floor_plan_images if 'data:image' not in x]
            external_link = response.url+'#'+str(j)
            j += 1

            property_type = 'house' if 'house' in description.lower(
            ) or 'house' in title.lower() else 'apartment'

            if int(rent) > 0 and int(rent) < 20000:
                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
                item_loader.add_value(
                    "external_source", self.external_source)  # String

                # item_loader.add_value("external_id", str(external_id))  # String
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
                item_loader.add_value("property_type", property_type)  # String
                item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                # item_loader.add_value("bathroom_count", bathroom_count)  # Int

                #item_loader.add_value("available_date", available_date)

                get_amenities(description, amenity, item_loader)

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value(
                    "external_images_count", len(images))  # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                item_loader.add_value("deposit", deposit)  # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "CAD")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int
                # item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value(
                    "landlord_name", 'Great Apartments')  # String
                item_loader.add_value(
                    "landlord_phone", landlord_phone)  # String
                # item_loader.add_value("landlord_email", '')  # String

                self.position += 1
                yield item_loader.load_item()
