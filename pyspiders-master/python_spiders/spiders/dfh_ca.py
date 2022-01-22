# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class DfhSpider(Spider):
    name = 'dfh_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.dfh.ca"]
    start_urls = ["https://www.dfh.ca/rentals/"]

    def parse(self, response):
        for url in response.css("div.col-3 a::attr(href)").getall():
            yield Request(url = url, callback = self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_state = response.css("li:contains('Available:') strong::text").get()
        if("Rented" in property_state):
            return
        
        available_date = property_state
        title = response.css("div.property-title h1::text").get()
        rent = response.css("div.property-title h4::text").get()
        rent = rent.split("/")[0]
        rent = "".join(rent.split(","))
        currency = "CAD"
        
        property_type = response.css("li:contains('Rental Type:') strong::text").get().lower()
        if("house" in property_type):
            property_type = "house"
        else:
            property_type = "apartment"
        
        external_id = response.css("div.property-lister span:contains('Rental #')").re("Rental #([0-9]+)")[0]

        square_meters = response.css("span:contains('sqft') strong::text").get()
        if(square_meters):
            square_meters = int(int(square_meters) / 10.7639)


        room_count = response.css("span:contains('Beds') strong::text").get()
        bathroom_count = response.css("span:contains('Baths') strong::text").get()
        bathroom_count = int(float(bathroom_count))

        furnished = response.css("li:contains('Furnished:') strong::text").get()
        if("No" in furnished):
            furnished = False
        else:
            furnished = True

        pets_allowed = response.css("li:contains('Pet Considered:') strong::text").get()
        if("No" in pets_allowed):
            pets_allowed = False
        else:
            pets_allowed = True

        description = response.css("div.property-description p::text").getall()
        description = " ".join(description)
        
        images = response.css("img.object-cover::attr(data-lazy)").getall()
        location_script = response.css("div#map script::text").get()
        latitude = re.findall('"lat":(-?[0-9]+\.[0-9]+)', location_script)[0]
        longitude = re.findall('"lng":(-?[0-9]+\.[0-9]+)', location_script)[0]

        landlord_name = "dfh"
        landlord_phone = "250-477-7291"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("description", description)
        item_loader.add_value("images", images)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
       
        yield item_loader.load_item()
