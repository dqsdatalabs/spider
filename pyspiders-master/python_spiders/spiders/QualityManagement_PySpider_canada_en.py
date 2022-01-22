# -*- coding: utf-8 -*-
# Author: Marwan Eid
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates, extract_rent_currency, extract_number_only

def find_nth(string, char, n):
    start = string.find(char)
    while start >= 0 and n > 1:
        start = string.find(char, start+len(char))
        n -= 1
    return start

class QualitymanagementPyspiderCanadaEnSpider(scrapy.Spider):
    #name = "QualityManagement_PySpider_canada_en"
    name = "qualitymanagement"
    start_urls = ['https://qualitymanagement.net/locations/?wpp_search%5Bpagination%5D=off&wpp_search%5Bper_page%5D=20&wpp_search%5Bstrict_search%5D=false&wpp_search%5Bproperty_type%5D=building%2Cappartment_&wpp_search%5Bavailability%5D=-1&wpp_search%5Blocation%5D=-1&wpp_search%5Bprice%5D%5Bmin%5D=&wpp_search%5Bprice%5D%5Bmax%5D=']
    allowed_domains = ["qualitymanagement.net"]
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
        for item in response.xpath("//li[contains(@class,'property_title')]//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.url[find_nth(str(response.url), "/", 4) + 1: -1]
        title = response.xpath("//h1[contains(@class,'jupiterx-main-header-post-title')]/text()").getall()[0]
        temp_desc = response.xpath("//div[contains(@class,'wpp_the_content')]").getall()[0]
        description = temp_desc[temp_desc.find('<p>') + 3: temp_desc.find('</p>')]
        if "<strong>" in description:
            index1 = find_nth(temp_desc, "<p>", 2) + 3
            index2 = find_nth(temp_desc, "</p>", 2)
            description = temp_desc[index1: index2]
        address = response.xpath("//li[contains(@class,'property_location wpp_stat_plain_list_location alt')]//span[contains(@class, 'value')]/text()").getall()[0]
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
        zipcode, city, _ = extract_location_from_coordinates(longitude, latitude)
        p_type = response.xpath("//li[contains(@class,'property_tagline wpp_stat_plain_list_tagline')]//span[contains(@class, 'value')]/text()").getall()[0]
        if "bedroom" in p_type or "room" in p_type:
            property_type = "room"
        elif "Suite" in p_type:
            property_type = "studio"
        room_count = response.xpath("//li[contains(@class,'property_bedrooms wpp_stat_plain_list_bedrooms alt')]//span[contains(@class, 'value')]/text()").getall()[0]
        if ("Bachelor" in room_count):
            room_count = 1
        else:
            room_count = int(room_count)
        pets = response.xpath("//li[contains(@class,'property_pets wpp_stat_plain_list_pets ')]//span[contains(@class, 'value')]/text()").getall()[0]
        if ("No Pets" in pets):
            pets_allowed = False
        else:
            pets_allowed = True
        longer_description = response.xpath("//div[contains(@class,'wpp_the_content')]").extract()[0]
        if "parking" in longer_description:
            parking = True
        else:
            parking = False
        if "Balcony" in longer_description or "balconies" in longer_description:
            balcony = True
        else:
            balcony = False
        if "Washer" in longer_description:
            washing_machine = True
        else:
            washing_machine = False
        if "dishwasher" in longer_description:
            dishwasher = True
        else:
            dishwasher = False

        images = response.xpath("//div[contains(@id,'qm-photos')]//a/@href").extract()
        floor_plan_images = response.xpath("//div[contains(@id,'qm-fp')]//a/@href").extract()

        rent_currency = response.xpath("//li[contains(@class,'property_price wpp_stat_plain_list_price ')]//span[contains(@class, 'value')]/text()").getall()[0]
        rent, currency = extract_rent_currency(rent_currency, self.external_source, QualitymanagementPyspiderCanadaEnSpider)
        deposit_currency = response.xpath("//li[contains(@class,'property_deposit wpp_stat_plain_list_deposit ')]").extract()
        if len(deposit_currency) > 0:
            deposit_currency = response.xpath("//li[contains(@class,'property_deposit wpp_stat_plain_list_deposit ')]//span[contains(@class, 'value')]/text()").getall()[0]
            deposit = extract_number_only(deposit_currency, ",")
        else:
            deposit = 0
        deposit = int(float(deposit))

        landlord_name = "Quality Management"
        landlord_number = response.xpath("//li[contains(@class,'property_phone_number wpp_stat_plain_list_phone_number ')]//span[contains(@class, 'value')]/text()").getall()[0]
        landlord_email = response.xpath("//li[contains(@class,'property_email wpp_stat_plain_list_email ')]//span[contains(@class, 'value')]//a/@href").getall()[0]
        landlord_email = landlord_email[7:]
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("currency", "CAD") # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
