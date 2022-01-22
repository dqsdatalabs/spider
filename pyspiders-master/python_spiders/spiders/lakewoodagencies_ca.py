# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy

from ..helper import extract_location_from_address, extract_location_from_coordinates, string_found
from ..loaders import ListingLoader


class LakewoodagenciesCaSpider(scrapy.Spider):
    name = "lakewoodagencies_ca"
    start_urls = [
        'https://lakewoodagencies.ca/',
    ]
    country = 'canada'  # Fill in the Country's name
    locale = 'ca'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="et_builder_inner_content et_pb_gutters3"]//a/@href').getall()
        urls = []
        for link in links:
            if "https" in link:
                urls.append(link)

        yield from response.follow_all(urls, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        pro_type = response.xpath('//div[@class="et_pb_text_inner"]/p/text()').get()
        if "Apartments" in pro_type:
            property_type = "apartment"
        else:
            property_type = "house"

        data = response.xpath(
            '//div[@class="et_pb_column et_pb_column_1_2 et_pb_column_1  et_pb_css_mix_blend_mode_passthrough et-last-child"]//div[@class="et_pb_text_inner"]//text()').getall()
        longitude, latitude = extract_location_from_address(data[1])
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        external_id = response.url.split("=")
        landlord_phone = response.xpath(
            '//div[@class="et_pb_column et_pb_column_1_2 et_pb_column_1  et_pb_css_mix_blend_mode_passthrough et-last-child"]//div[@class="et_pb_text_inner"]//a[last()]/@href').get().split(
            ":")[1]
        rent = response.xpath(
            '//div[@class="et_pb_column et_pb_column_1_2 et_pb_column_0  et_pb_css_mix_blend_mode_passthrough"]//p[2]//text()').getall()[
            -1].replace(',', '.')
        description = response.xpath(
            '//div[@class="et_pb_row et_pb_row_1"]//div[@class="et_pb_text_inner"]//text()').getall()
        room_count = response.xpath(
            '//div[@class="et_pb_column et_pb_column_1_2 et_pb_column_0  et_pb_css_mix_blend_mode_passthrough"]//p[2]//text()').get().split(
            "Bedroom")[0].strip()
        title = response.xpath('//div[@class="et_pb_column et_pb_column_1_2 et_pb_column_0  et_pb_css_mix_blend_mode_passthrough"]//h4//text()').get()
        amenities = " ".join(description)

        terrace = False
        if string_found(['terrace', 'terraces'], amenities):
            terrace = True

        elevator = False
        if string_found(['elevator', 'elevators'], amenities):
            elevator = True

        parking = False
        if string_found(['parking'], amenities):
            parking = True

        dishwasher = False
        if string_found(['dishwasher'], amenities):
            dishwasher = True

        washing_machine = False
        if string_found(['Laundry', 'Washer'], amenities):
            washing_machine = True

        swimming_pool = False
        if string_found(['outdoor pool'], amenities):
            swimming_pool = True

        balcony = False
        if string_found(['Balcony'], amenities):
            balcony = True

        pets_allowed = False
        if string_found(['Pets Allowed', 'cat', 'dog'], amenities):
            pets_allowed = True

        # # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id[-1])  # String
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
        item_loader.add_value("property_type", property_type)  # String
        # item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count[-1])  # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        images = response.xpath('//div[@class="et_pb_gallery_image landscape"]//img/@src').getall()

        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details

        item_loader.add_value("landlord_name", "LAKEWOOD AGENCIES") # String
        item_loader.add_value("landlord_phone", landlord_phone)  # String
        item_loader.add_value("landlord_email", "propertymgmt@ladcocompany.com") # String

        self.position += 1
        yield item_loader.load_item()
