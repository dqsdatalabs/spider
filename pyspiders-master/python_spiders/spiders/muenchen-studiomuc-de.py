# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy
from ..loaders import ListingLoader
from ..helper import string_found, convert_string_to_numeric, remove_white_spaces, extract_location_from_address, extract_location_from_coordinates


class MuenchenStudiomuc(scrapy.Spider):
    name = "muenchen_studiomuc"
    allowed_domains = ['muenchen.studiomuc.de']
    country = 'Germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    start_urls = ['https://muenchen.studiomuc.de/vermietung.htm']

    position = 1

    # 1. SCRAPING level 1
    # def start_requests(self):
    #     for url in self.start_urls:
    #         yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response):
        apartment_page_links = response.xpath('//div[@class="itemwrapper"]//a[@class="button"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Your scraping code goes here
        # Dont push any prints or comments in the code section
        # if you want to use an item from the item loader uncomment it
        # else leave it commented
        # Finally make sure NOT to use this format
        #    if x:
        #       item_loader.add_value("furnished", furnished)
        # Use this:
        #   balcony = None
        #   if "balcony" in description:
        #       balcony = True

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_xpath("title", '//div[@id="apartmentDetail"]//h1//text()') # String

        # # Property Details
        appertment="".join(response.xpath('//div[@class="teaser"]//ul/li//text()').getall())
        Highlights ="".join(response.xpath('//div[@class="highlights"]//text()').getall())
        description = remove_white_spaces(Highlights + appertment)

        item_loader.add_value("description", description) # String
        address = response.xpath('//div[@class="free"]//text()').getall()
        longitude, latitude = extract_location_from_address("".join(address))

        item_loader.add_value("city", 'Munich') # String
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address[2:])  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment")  # String
        item_loader.add_xpath("square_meters", '//tr[1]//td[@class="val"]//text()')  # Int

        room_count = response.xpath('//tr[2]//td[@class="val"]//text()').get()
        roomcount = convert_string_to_numeric(room_count, MuenchenStudiomuc)
        if roomcount == 1.5:
            item_loader.add_value("room_count", 2)  # Int
        else:
            item_loader.add_value("room_count", roomcount)  # Int


        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean

        if "Möblierung" in Highlights:
            item_loader.add_value('furnished', True)
        else:
            item_loader.add_value('furnished', False)

        item_loader.add_value("parking", True) # Boolean
        # item_loader.add_value('elevator', elevator)  # Boolean

        balcony = ['Balkon']
        item_loader.add_value('balcony', string_found(balcony, appertment))

        if "Terrasse" in appertment:
            item_loader.add_value('terrace', True)
        else:
            item_loader.add_value('terrace', None)

        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        images = response.xpath('body[@id="cms"]//div[@id="ambienceSlider"]/div/@style').getall()
        images = [img.replace("background-image:url(", "").replace(");", "") for img in images]
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        rent = response.xpath('//tr[3]//td[@class="val"]//text()').get()
        item_loader.add_value("rent", rent)  # Int

        depo = convert_string_to_numeric(rent, MuenchenStudiomuc)
        item_loader.add_value("deposit", depo*3)  # Int

        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_xpath("utilities", '//tr[4]//td[@class="val"]//text()') # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", ) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Mr. Nick Hamm')  # String
        item_loader.add_value("landlord_phone", ' +49(157)56122596 - +49 (941)92028-44')  # String
        item_loader.add_value("landlord_email", ' rental-muc@eukia-hausverwaltung.de')  # String

        self.position += 1
        yield item_loader.load_item()
