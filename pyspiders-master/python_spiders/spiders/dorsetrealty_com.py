# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class DorsetRealtySpider(Spider):
    name = 'dorsetrealty_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.dorsetrealty.com"]
    start_urls = ["https://dorsetrealty.com/residential/"]

    def parse(self, response):
        for url in response.css("div.div_properties_listing_left2 a::attr(href)").getall():
            yield Request(url = url, callback = self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_state = response.css("div.p_20::text").getall()
        property_type = property_state[0].strip().split("|")[0]
        address = property_state[1].strip()
        property_state = property_state[0].strip().split("|")[1]
        if("AVAILABLE" not in property_state):
            return
        title = property_state
        property_type = property_type.lower().strip()
        if("house" in property_type):
            property_type = "house"

        city = address.split(",")[1]
        description = response.css("div#div_prop_info p::text").getall()
        description = " ".join(description)
        
        rent = response.css(".div_unit_info_1 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(2)::text").get()
        if not rent:
            rent = response.css(".div_unit_info_1 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > div:nth-child(2)::text").get()
        if rent:
            rent = re.findall("[0-9]+", rent)[0]
        currency = "CAD"

        square_meters = response.css(".div_unit_info_1 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(5)::text").get()
        if not square_meters:
            square_meters = response.css("div.d_table_cell_5:nth-child(5)::text").get()
        
        if(square_meters):
            square_meters = re.findall("[0-9]+", square_meters)[0]
            square_meters = int(square_meters) / 10.7639
            square_meters = int(square_meters)
        room_count = response.css(".div_unit_info_1 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(3)::text").get()
        if (not room_count):
            room_count = response.css(".div_unit_info_1 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > div:nth-child(3)::text").get()
        if(room_count):
            room_count = re.findall("[0-9]+", room_count)[0]

        bathroom_count = response.css(".div_unit_info_1 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(4)::text").get()
        if (not bathroom_count):
            bathroom_count = response.css("div.d_table_cell_5:nth-child(4)::text").get()
        if(bathroom_count):
            bathroom_count = re.findall("[0-9]+", bathroom_count)[0]

        available_date = response.css(".div_unit_info_1 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(6)::text").get()
        if (not available_date):
            available_date = response.css("div.d_table_cell_5:nth-child(4)::text").get()
        if("Bathrooms" in available_date):
            available_date = response.css("div.d_table_cell_5:nth-child(6)::text").get()

        images = response.css("div.galleria  img::attr(src)").getall()
        external_id = response.css("link[rel='shortlink']::attr(href)").get().split('=')[1]

        landlord_name = "DORSETREALTY"
        landlord_phone = "604-270-1711"
        landlord_email = "leasecoordinator@dorsetrealty.com"

        property_information = response.css("div#div_prop_info ul li::text").getall()

        washing_machine = None
        pets_allowed = None

        if(len(property_information) > 0):
            pets_allowed = property_information[1]
            washing_machine = property_information[2]

            if("No" in pets_allowed):
                pets_allowed = False
            else:
                pets_allowed = True
            
            if(washing_machine):
                washing_machine = True
            else:
                washing_machine = False

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("description", description)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("images", images)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("pets_allowed", pets_allowed)
       
        yield item_loader.load_item()
