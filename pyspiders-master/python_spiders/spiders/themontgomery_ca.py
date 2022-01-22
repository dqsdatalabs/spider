# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address , extract_number_only, format_date



class ThemontgomeryCaSpider(scrapy.Spider):
    name = "themontgomery_ca"
    start_urls = ['https://www.themontgomery.ca/apartmentsearchresult.aspx?Bed=-1&rent=&MoveInDate=&myOlePropertyId=1310326&UnitCode=&control=1']
    country = 'canada' # Fill in the Country's name
    locale = 'ca' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    # def start_requests(self):
    #     for url in self.start_urls:
    #         yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response):
        items = response.xpath('//div[@id="innerformdiv"]/div[@class="row-fluid"]//div[@class="block"]//table/tbody/tr')
        for item in items:
            links = item.xpath('.//td[@data-label="Action"]/input/@onclick').get().replace("SetTermsUrl('", '').replace("')", " ")
            yield scrapy.Request("https://www.themontgomery.ca/{}".format(links),
                                 callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # # MetaData
        title = response.xpath('//h1[@class="text-normal m-n text-4xl"]//text()').getall()
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", title[0]) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title",  "".join(title)) # String
        #item_loader.add_value("description", description) # String

        # # Property Details
        room, bathroom = response.xpath('//span[@data-selenium-id="TermsFPBedBath"]/text()').get().split("/")
        square_meters = response.xpath('//li/span[@data-selenium-id="TermsFPSQFT"]/text()').get().replace(',', '')
        available_date = response.xpath('//input[@id="MoveInDate"]/@value').get()
        address = response.xpath('//address[@id="address"]//span[@itemprop="address"]//text()').getall()
        longitude, latitude = extract_location_from_address(" ".join(address))

        item_loader.add_xpath("city", '//span[@data-selenium-id="address_city"]/text()')  # String
        item_loader.add_xpath("zipcode", '//span[@data-selenium-id="address_zip"]/text()')  # String
        item_loader.add_value("address", "".join(address).replace(". ", ""))  # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment")  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", extract_number_only(room))  # Int
        item_loader.add_value("bathroom_count", extract_number_only(bathroom))  # Int
        item_loader.add_value("available_date", format_date(available_date))  # String => date_format
        item_loader.add_value("pets_allowed", True)  # Boolean
        item_loader.add_value("furnished", True)  # Boolean
        item_loader.add_value("parking", True)  # Boolean
        # item_loader.add_value("elevator",elevator ) # Boolean
        item_loader.add_value('balcony', True)  # Boolean
        item_loader.add_value("terrace", True)  # Boolean
        item_loader.add_value("swimming_pool", True)  # Boolean
        item_loader.add_value("washing_machine", True)  # Boolean
        item_loader.add_value("dishwasher", True)  # Boolean

        # # Images
        #item_loader.add_value("images", images) # Array
        #item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        rent = response.xpath('//div[@class="ysp"]//text()').get().replace(',', '')

        item_loader.add_value("rent", rent)  # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "USD")  # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int
        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'The Montgomery')  # String
        item_loader.add_value("landlord_phone", '(647) 945-5243')  # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
