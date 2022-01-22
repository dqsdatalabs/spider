# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy
import re
from ..loaders import ListingLoader
from ..helper import extract_number_only, string_found, remove_white_spaces, extract_location_from_address, extract_location_from_coordinates, remove_unicode_char, convert_to_numeric, format_date


class BannaschDeSpider(scrapy.Spider):
    name = "bannasch_de"
    start_urls = ['https://www.bannasch.de/immobilien-mieten?search=&property_type=wohnung']
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1


    # 2. SCRAPING level 2
    def parse(self, response):
        apartment_page_links = response.xpath('//a[@class="content-link"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)



    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        check1 = response.xpath('//div[@class="col col-xs-12"]/ul//li[4]//span[1]//text()').get()
        check2 = response.xpath('//div[@class="col col-xs-12"]/ul//li[7]//span[1]//text()').get()
        check3 = response.xpath('//div[@class="col col-xs-12"]/ul//li[6]//span[1]//text()').get()

        if check1 == "Wohnfläche:" and check3 == "Nebenkosten:" :
            square_meters = response.xpath('//div[@class="col col-xs-12"]/ul//li[4]//span[2]//text()').get().replace(".",",")
            room_count = convert_to_numeric(response.xpath('//div[@class="col col-xs-12"]/ul//li[5]//span[2]//text()').get())
            utilities = response.xpath('//div[@class="col col-xs-12"]/ul//li[6]//span[2]//text()').get()
            deposit = response.xpath('//div[@class="col col-xs-12"]/ul//li[7]//span[2]//text()').get()
            rent = remove_white_spaces(" ".join(response.xpath('//div[@class="col col-xs-12"]/ul//li[8]//text()').getall()))

        elif check2 == "Nebenkosten:":
            square_meters = response.xpath('//div[@class="col col-xs-12"]/ul//li[5]//span[2]//text()').get().replace(".", ",")
            room_count = convert_to_numeric(response.xpath('//div[@class="col col-xs-12"]/ul//li[6]//span[2]//text()').get())
            utilities = response.xpath('//div[@class="col col-xs-12"]/ul//li[7]//span[2]//text()').get()
            deposit = response.xpath('//div[@class="col col-xs-12"]/ul//li[8]//span[2]//text()').get()
            rent = remove_white_spaces(" ".join(response.xpath('//div[@class="col col-xs-12"]/ul//li[9]//text()').getall()))

        else:
            square_meters = response.xpath('//div[@class="col col-xs-12"]/ul//li[5]//span[2]//text()').get().replace(".", ",")
            room_count = convert_to_numeric(response.xpath('//div[@class="col col-xs-12"]/ul//li[6]//span[2]//text()').get())
            utilities = response.xpath('//div[@class="col col-xs-12"]/ul//li[8]//span[2]//text()').get()
            deposit = response.xpath('//div[@class="col col-xs-12"]/ul//li[9]//span[2]//text()').get()
            rent = remove_white_spaces(" ".join(response.xpath('//div[@class="col col-xs-12"]/ul//li[10]//text()').getall()))

        external_id = extract_number_only(response.xpath('//div[@class="col col-xs-12"]/ul//li[1]//span[2]//text()').get())
        prop_type = " ".join(response.xpath('//div[@class="col col-xs-12"]/ul//li[2]//text()').getall())
        address = remove_white_spaces(" ".join(response.xpath('//div[@class="col col-xs-12"]/ul//li[3]//text()')[3:].getall()))

        title = remove_unicode_char(response.xpath('//div[@class="col col-xs-12 headline_wrapper"]/h1/text()').get())
        description = remove_unicode_char(" ".join(response.xpath('//div[@id="tab-1"]/text()')[:-1].getall()))
        amenities = remove_white_spaces(" ".join(response.xpath('//div[@id="tab-2"]/text()').getall()))
        bathroom_count = amenities.count('Badezimmer')
        if bathroom_count == 0:
            bathroom_count=+1

        longitude, latitude = extract_location_from_address(address)
        zipcode, city, add = extract_location_from_coordinates(longitude, latitude)
        landlord_name = remove_unicode_char(response.xpath('//div[@class="partner-teaser"]/h3/text()').get())

        if string_found(['Erdgeschoss', 'Obergeschoss', 'Ober', 'Dachgeschoss'], response.xpath('//div[@id="tab-2"]/text()')[2].get()):
             floor = response.xpath('//div[@id="tab-2"]/text()')[2].get()
        elif string_found(['Erdgeschoss', 'Obergeschoss', 'Ober', 'Dachgeschoss'], response.xpath('//div[@id="tab-2"]/text()')[3].get()):
            floor = response.xpath('//div[@id="tab-2"]/text()')[3].get()
        else:
            floor = None

        available_date = None
        if string_found(['frei ab', 'bezugsfertig ab', 'Bezug ab'], response.xpath('//div[@id="tab-2"]/text()')[6].get()):
            date = remove_unicode_char(response.xpath('//div[@id="tab-2"]/text()')[6].get())
            available_date = remove_white_spaces(re.sub(r'[a-z, A-Z, -]', ' ', date)).replace(".", "/")

        if type(room_count) is float:
            room_count = room_count+0.5

        if string_found(['Maisonette-Wohnung', 'Terassenwohnung', 'Erdgeschosswohnung'], prop_type):
            property_type = "apartment"
        else:
            property_type = "flat"
        furnished = False
        if string_found(['voll möbliert'], amenities):
            furnished = True
        parking = False
        if string_found(['Garage', 'Außenstellplatz', 'Tiefgaragenstellplatz', 'Stellplatz', 'Einzelgarage'], amenities):
            parking = True
        elevator = False
        if string_found(['Aufzug'], amenities):
            elevator = True
        balcony = False
        if string_found(['Balkon', 'Südbalkon'], amenities):
            balcony = True
        terrace = False
        if string_found(['Terrassenwohnung', 'Terrasse'], amenities):
            terrace = True
        washing_machine = False
        if string_found(['gemeinschaftlicher Wasch', 'Trockenraum', 'Waschküche'], amenities):
            washing_machine = True
        dishwasher = False
        if string_found(['Spülmaschine'], amenities):
            dishwasher = True



        # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id",external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", remove_unicode_char(city))  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", remove_unicode_char(add))  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("available_date", format_date(available_date)) # String => date_format
        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        images = response.xpath('//div[@class="object-item"]//img[not(@alt="Bannasch Immobilien")]//@data-src').getall()

        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count",  len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit ) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String
        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int
        item_loader.add_value("energy_label", amenities[-1]) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_xpath("landlord_phone", '//div[@class="partner-teaser"]/ul[@class="partner-contacts is-desktop"]//li[1]//span//text()')  # String
        item_loader.add_xpath("landlord_email", '//div[@class="partner-teaser"]/ul[@class="partner-contacts is-desktop"]//li[3]//span//text()')  # String

        self.position += 1
        yield item_loader.load_item()
