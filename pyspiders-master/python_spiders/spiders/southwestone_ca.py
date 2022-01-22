# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas

import scrapy

from ..helper import extract_location_from_address, string_found, extract_location_from_coordinates, \
    remove_white_spaces, extract_number_only, remove_unicode_char
from ..loaders import ListingLoader


class SouthwestoneCaSpider(scrapy.Spider):
    name = "southwestone_ca"
    start_urls = ['https://southwestone.ca/units/']
    country = 'canada'  # Fill in the Country's name
    locale = 'ca'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 2. SCRAPING level 2
    def parse(self, response):
        apartment_page_links = response.xpath('//a[@class="vc_gitem-link vc-zone-link"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        rent = response.xpath('//div[@class="wpb_text_column wpb_content_element  prix"]//p/text()').get()
        pro_type = response.xpath(
            '//div[@class="wpb_text_column wpb_content_element  mini-desrcipt"]//p/text()').get().split("(")
        external_id = response.xpath('//div[@class="wpb_column vc_column_container vc_col-sm-12"]//span/text()').get()
        square_meters = response.xpath(
            '//div[@class="wpb_text_column wpb_content_element  mini-desrcipt"]//p/text()').get().split("(")
        location = response.xpath('//div[@class="wpb_column vc_column_container vc_col-sm-3"]//p[2]//text()').getall()
        longitude, latitude = extract_location_from_address("".join(location))
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        description = response.xpath(
            '//div[@class="wpb_text_column wpb_content_element "]/div[@class="wpb_wrapper"]/p//text()').get()
        details = response.xpath('//div[@class="wpb_text_column wpb_content_element "]//ul//text()').getall()
        amenities = " ".join(details) + description
        title = response.xpath(
            '//div[@class="wpb_column vc_column_container vc_col-sm-12"]//h2//text()').get() + external_id

        bathroom_count = 1
        for item in range(len(details)):
            if "bathrooms" in details[item].lower():
                bathroom_count = extract_number_only(details[item])

        room_count = 1
        for item in details:
            if "bedrooms" in item.lower():
                room_count = extract_number_only(item)

        terrace = False
        if string_found(['terrace', 'terraces'], amenities):
            terrace = True
        balcony = False
        if string_found(['balcony', 'balconies'], amenities):
            balcony = True
        furnished = False
        if string_found(['furnished '], amenities):
            furnished = True

        property_type = "apartment"
        if string_found(['STUDIO'], pro_type[0]):
            property_type = "studio"
        elif string_found(['PENTHOUSE', 'TOWNHOUSE'], pro_type[0]):
            property_type = "house"

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", remove_unicode_char(title))  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", remove_white_spaces(location[-1]))  # String
        item_loader.add_value("address", location)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value("square_meters", square_meters[1])  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", True)  # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        floor_plan_images = response.xpath('//img[@class="vc_single_image-img attachment-full"]/@src').get()

        # item_loader.add_value("images", images) # Array
        # item_loader.add_value("external_images_count", len(images)) # Int
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
        item_loader.add_value("landlord_name", 'SOUTHWEST ONE')  # String
        item_loader.add_value("landlord_phone", '1-888-906-8162')  # String
        item_loader.add_value("landlord_email", 'info@southwestone.ca')  # String

        self.position += 1
        yield item_loader.load_item()
