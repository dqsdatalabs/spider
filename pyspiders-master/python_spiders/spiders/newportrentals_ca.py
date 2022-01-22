# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class NewportrentalsSpider(Spider):
    name = 'newportrentals_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.newportrentals.ca"]
    start_urls = ["https://www.newportrentals.ca/rentals"]

    def parse(self, response):
        for div in response.css("div.rental"):
            item_loader = ListingLoader(response=response)
            property_type = "Apartment"
            title = div.css("h2::text").get()
            
            description = div.css("p::text").getall()
            description = " ".join(description)

            rent = re.findall("Price: (.+) \s", description)[0]
            if("," in rent):
                rent = rent.split(",")
                rent = "".join(rent)
            rent = re.findall("([0-9]+)", rent)[0]
            currency = "CAD"

            room_count = re.findall("([0-9]) Bed\(s\)", div.get())[0]
            bathroom_count = re.findall("([0-9]) Baths\(s\)", div.get())[0]
            available_date = div.css("p.available::text").get()
            
            square_meters = re.findall("([0-9]+) sqft", div.get())
            if(len(square_meters) > 0):
                square_meters = square_meters[0]
                square_meters = int(int(float(square_meters))/10.763)
            else: 
                square_meters = None

            images = div.css("div.rental-pics a::attr(href)").getall()
            images = [f"{self.allowed_domains[0]}{image_src}" for image_src in images]

            external_id = div.css("a.rental-inquiry::attr(data-id)").get()
            address = div.css("a.rental-inquiry::attr(data-address)").get()
            pets_allowed = False

            landlord_name = "newportrentals"
            landlord_phone = "250.598.2220"
            landlord_email = "info@newportrentals.ca"

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("rent_string", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("available_date", available_date)
            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            item_loader.add_value("images", images)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("address", address)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("landlord_email", landlord_email)
        
            yield item_loader.load_item()
