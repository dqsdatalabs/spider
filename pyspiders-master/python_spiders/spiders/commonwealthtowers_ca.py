# -*- coding: utf-8 -*-
# Author: Abdelrahman Abbas
import scrapy

from ..helper import remove_white_spaces, string_found, extract_location_from_coordinates
from ..loaders import ListingLoader


class CommonwealthtowersCaSpider(scrapy.Spider):
    name = "commonwealthtowers_ca"
    start_urls = ['https://www.commonwealthtowers.ca/']
    country = 'canada'  # Fill in the Country's name
    locale = 'ca'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    # def start_requests(self):
    #     for url in self.start_urls:
    #         yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2

    # 3. SCRAPING level 3
    def parse(self, response):
        url1 = response.xpath('//div[@class="suites_panel"]//div[@class="tab-content"]//div[@role="tabpanel"]')
        url2 = response.xpath('//div[@class="suites_panel"]//ul[@class="nav nav-tabs"]//li')
        links = []
        props = []
        for items in url2:
            link = items.xpath('.//a[@class="no_scroll h3-styles"]/@href').get()
            links.append(link)
            prop = remove_white_spaces(items.xpath('.//a[@class="no_scroll h3-styles"]/text()').get())
            props.append(prop)

        for items in url1:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link",
                                  f"https://www.commonwealthtowers.ca/{links[self.position - 1]}")  # String
            item_loader.add_value("external_source", self.external_source)  # String
            description = response.xpath('//div[@class="about_details_container"]//p/text()').get().replace(
                'www.commonwealthtowers.ca', '')

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", "40 & 60 PLEASANT BLVD")  # String
            item_loader.add_value("description", description)  # String

            # Property Details
            # variables
            rent = items.xpath(
                './/div[@class="suite_details_box first suite_rent"]//span[@class="suite_cell_value h4-styles type-accent"]/span/text()').get()
            square_meters = items.xpath(
                './/div[@class="suite_details_box suite_sqft"]//span[@class="suite_cell_value h4-styles type-accent"]/span/text()').get()
            suite_amenities = " ".join(items.xpath('//div[@class="suite-amenities"]/ul//text()').getall())
            building_amenities = " ".join(items.xpath('//div[@class="building-amenities split"]/ul//text()').getall())
            latitude = items.xpath('//div[@class="row"]/div[@id="neighbourhood-map-wrap"]/@data-latitude').get()
            longitude = items.xpath('//div[@class="row"]/div[@id="neighbourhood-map-wrap"]/@data-longitude').get()
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            # Items
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", latitude)  # String
            item_loader.add_value("longitude", longitude)  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("bathroom_count", 1)  # Int
            #
            if props[self.position - 1] == "Bachelor":
                item_loader.add_value("room_count", 1)  # Int
                item_loader.add_value("property_type", "room")  # String
            elif "Bedrooms" in props[self.position - 1]:
                item_loader.add_value("room_count", 2)  # Int
                item_loader.add_value("property_type", "apartment")  # String
            else:
                item_loader.add_value("room_count", 1)  # Int
                item_loader.add_value("property_type", "apartment")  # String

            # item_loader.add_value("available_date", available_date) # String => date_format
            # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", string_found(['Parks', 'parking'], building_amenities))  # Boolean
            item_loader.add_value("elevator", string_found(['Elevators'], building_amenities))  # Boolean
            item_loader.add_value('balcony', string_found(['Balconies'], suite_amenities))  # Boolean
            item_loader.add_value("terrace", string_found(['Terrace'], building_amenities))  # Boolean
            item_loader.add_value("swimming_pool", string_found(['pool'], building_amenities))  # Boolean
            item_loader.add_value("washing_machine", string_found(['Laundry '], building_amenities))  # Boolean
            item_loader.add_value("dishwasher", string_found(['Dishwasher'], suite_amenities))  # Boolean

            # Images
            # variables
            images = items.xpath('.//div[@class="suite_images_container"]/a/@href').getall()
            floor_plan_images = items.xpath('.//div[@class="suite_floorplans_container"]/a/@href').get()

            # Items
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
            item_loader.add_value("landlord_name", "Yonge Pleasant Holdings Ltd")  # String
            item_loader.add_value("landlord_phone", '(416) 825-8894')  # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
