# -*- coding: utf-8 -*-
# Author: A.Abbas
import scrapy

from ..helper import extract_number_only, extract_location_from_coordinates, string_found
from ..loaders import ListingLoader


class MyrentalCaSpider(scrapy.Spider):
    name = "myrental_ca"
    start_urls = [
        "https://www.myrental.ca/"
    ]
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
        for item in response.xpath('//div[@class="communities"]//div[@class="property-item"]'):
            url = item.xpath('.//div[@class="property-content"]/a//@href').get()
            link = "https://www.myrental.ca{}".format(url)
            address = item.xpath(
                './/div[@class="property-content"]//div[@class="property-address click"]//p//text()').get()
            company_name = item.xpath('.//div[@class="property-content"]//h3[@class="click"]//text()').get()
            yield scrapy.Request("https://www.myrental.ca{}".format(url), callback=self.populate_item,
                                 meta={"address": address, "link": link, "company_name": company_name, })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        landlord_name = response.meta["company_name"]

        office_details = response.xpath('//div[@class="office_details"]//text()').getall()
        landlord_number = None
        for i in office_details:
            if "Tel:" in i:
                landlord_number = i.replace("Tel:", '')
            if "Phone:" in i:
                landlord_number = i.replace("Phone:", '')
        landlord_email = None
        for i in office_details:
            if "Email:" in i:
                landlord_email = i.replace("Email:", '')

        ad_link = response.xpath('//div[@class="suites_panel"]//ul[@class="nav nav-tabs"]//li//a/@href').getall()

        description = response.xpath('//div[@class="about_details_container"]//p/text()').get()

        details1 = response.xpath('//div[@class="custom-amenities-container"]//text()').getall()

        amenities = " ".join(details1)

        washing_machine = False
        if string_found(['Laundry'], amenities):
            washing_machine = True

        furnished = False
        if string_found(['Available furnished'], amenities):
            furnished = True

        balcony = False
        if string_found(['Balcony'], amenities):
            balcony = True

        parking = False
        if string_found(['Parking'], amenities):
            parking = True

        elevator = False
        if string_found(['elevator', 'elevators'], amenities):
            elevator = True

        terrace = False
        if string_found(['terrace'], amenities):
            terrace = True

        pets_allowed = False
        if string_found(['Pet Friendly'], amenities):
            pets_allowed = True

        swimming_pool = False
        if string_found(['pool', "swimming pool", 'swimming'], amenities):
            swimming_pool = True

        dishwasher = False
        if string_found(['dishwasher'], amenities):
            dishwasher = True

        counter = 0

        for items in response.xpath('//div[@class="suite_details_panel"]/div'):

            latitude = items.xpath('//div[@class="row"]/div[@id="neighbourhood-map-wrap"]/@data-latitude').get()
            longitude = items.xpath(
                '//div[@class="row"]/div[@id="neighbourhood-map-wrap"]/@data-longitude').get()
            zipcode, city, location = extract_location_from_coordinates(longitude, latitude)

            check = items.xpath('.//p//@class').get()

            if "wait-list-text" != check:

                rent = items.xpath(
                    './/div[@class="suite_details_box first suite_rent"]//span[@class="suite_cell_value h4-styles type-accent"]/span/text()').get()
                square_meters = items.xpath(
                    './/div[@class="suite_details_box suite_sqft"]//span[@class="suite_cell_value h4-styles type-accent"]/span/text()').get()

                bathroom = items.xpath('.//div[@class="suite_details_box suite_bath"]//span[@class="suite_cell_value h4-styles type-accent"]/text()').get().lower()
                bathroom_count = extract_number_only(bathroom)
                if bathroom is not None and "." in bathroom:
                    bathroom_count = float(bathroom.replace("baths",""))
                    bathroom_count += 0.5

                images = items.xpath('.//div[@class="suite_images_container"]/a/@href').getall()

                floor_plan_images = items.xpath('.//div[@class="suite-floorplans"]//a//@href').get()

                id = floor_plan_images.split("/")[-1].replace(".pdf", "")

                property_type = "apartment"
                if "_ph" in floor_plan_images:
                    property_type = "house"

                room_count = 1
                if "_2" in floor_plan_images:
                    room_count = 2

                title = f'{landlord_name}: {room_count} Bedroom'

                # MetaData
                item_loader = ListingLoader(response=response)

                item_loader.add_value("external_link", f"{response.url}/{id}")  # String
                item_loader.add_value("external_source", self.external_source)  # String

                # item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", response.meta["address"])  # String
                item_loader.add_value("latitude", str(latitude))  # String
                item_loader.add_value("longitude", str(longitude))  # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type)  # String
                item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                # item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
                item_loader.add_value("furnished", furnished)  # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                item_loader.add_value("terrace", terrace)  # Boolean
                item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label) # String
                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name)  # String
                item_loader.add_value("landlord_phone", landlord_number)  # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
                counter += 1
