# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy

from ..helper import extract_location_from_address, extract_location_from_coordinates, extract_date, string_found,extract_last_number_only, convert_to_numeric, remove_white_spaces

from ..loaders import ListingLoader


class CamdenpmComSpider(scrapy.Spider):
    name = "camdenpm_com"
    start_urls = ['https://camdenpm.managebuilding.com/Resident/public/rentals']
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
        apartment_page_links = response.xpath('//div[@class="rentals"]//a[@class="featured-listing accent-color-border-on-hover"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        address = response.xpath('//h1[@class="title title--medium title--margin-top title--margin-bottom"]//text()').get()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, location = extract_location_from_coordinates(longitude, latitude)

        rent = response.xpath('//div[@class="unit-detail__info column column--md6 column--sm12"]//div[@class="unit-detail__price accent-color title--small"]//text()').get().replace(',','.').replace('.00',"")
        available_date = remove_white_spaces(response.xpath('//div[@class="unit-detail__info column column--md6 column--sm12"]//div[@class="unit-detail__available-date text--muted"]//text()').get().replace("Available",''))

        room_count = extract_last_number_only(response.xpath('//ul[@class="unit-detail__unit-info"]//li[1]//text()').get())

        bathroom = response.xpath('//ul[@class="unit-detail__unit-info"]//li[2]//text()').get()
        if "." in bathroom:
            bathroom_count = int(convert_to_numeric(extract_last_number_only(bathroom.replace(".5","")))) + 1
        else:
            bathroom_count = extract_last_number_only(bathroom)

        images = response.xpath('//ul[@class="js-gallery unseen"]//li//@data-mfp-src').getall()

        description = " ".join(response.xpath('//p[@class="unit-detail__description"]//text()').getall())
        details1 = response.xpath('//ul[@class="unit-detail__features-list"]//li//text()').getall()
        amenities = " ".join(details1) + description

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
        if string_found(['elevator'], amenities):
            elevator = True

        terrace = False
        if string_found(['terrace'], amenities):
            terrace = True

        # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", address)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment") # String
        # item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Camden Property Management") # String
        # item_loader.add_value("landlord_phone", landlord_number) # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
