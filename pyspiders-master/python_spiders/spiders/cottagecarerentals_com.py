# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class CottagecaRerentalsSpider(Spider):
    name = 'cottagecarerentals_com'
    country='canda'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.cottagecarerentals.com"]
    start_urls = ["https://www.cottagecarerentals.com/"]

    def parse(self, response):
        for url in response.css("div.text-center div.button a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        title = response.css("h1.details::text").get()
        
        rent = response.css("div.details div.text-center span.details-rate::text").get()
        if("-" in rent):
            rent = rent.split("-")[0]
        rent = rent.split(",")
        rent = "".join(rent)

        images = response.css("img.rsImg::attr(src)").getall()
        images = [response.urljoin(image_src) for image_src in images]

        room_count = response.css("li.bedrooms-lrg::text").get()
        room_count = re.findall("[0-9]+", room_count)[0]

        bathroom_count = response.css("li.bathrooms-lrg::text").get()
        bathroom_count = re.findall("[0-9]+", bathroom_count)[0]
        
        pets_allowed = response.css("li.no-pets-lrg::text").get()
        if pets_allowed:
            if pets_allowed.find("No") >= 0:
                pets_allowed = False
            else:
                pets_allowed = True


        washing_machine = response.css("li.laundry-lrg::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False

        dishwasher = response.css("li.dishwasher-lrg::text").get()
        if dishwasher:
            dishwasher = True
        else:
            dishwasher = False

        description1 = response.css("div.panel ul li::text").getall()
        description2 = response.css("div.panel ol li::text").getall()
        description1.extend(description2)
        description = " ".join(description1)
        description = re.sub("\s+", " ", description)

        address = response.css("ul.large-list33:nth-child(1) > li:nth-child(1)::text").get()
        location_script = response.css("script:contains('function renderMap()')::text").get().strip()
        latitude = re.findall("lat: (\-?[0-9]+\.[0-9]+)", location_script)[0]
        longitude = re.findall("lng: (\-?[0-9]+\.[0-9]+)", location_script)[0]


        landlord_name = "cottagecarerentals"
        landlord_phone = "705-457-3306"
        landlord_email = "info@cottagecarerentals.ca"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("description", description)
        item_loader.add_value("address", address)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
