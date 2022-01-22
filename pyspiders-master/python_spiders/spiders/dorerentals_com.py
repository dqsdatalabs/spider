# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class DoreRentalsSpider(Spider):
    name = 'dorerentals_com'
    country='canda'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.dorerentals.com"]
    start_urls = ["https://www.dorerentals.com/rental-listings.html"]

    def parse(self, response):
        for url in response.css("div.field-name-link a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = "apartment"
        rent = response.css("div.price::text").get()
        currency = "CAD"
        title = response.css("div#node-property-full-group-address > address:nth-child(1)::text").get()
        images = response.css("ul.slides li a::attr(href)").getall()
        room_count = response.css("div.field-name-field-property-bedrooms:contains('Bedrooms:')::text").get()
        bathroom_count = response.css(".field-name-baths > span:nth-child(2)::text").get().split(" ")[0]
        description = response.css("div.field-name-body::text").get()
        address = title
        city = address.split(" ")[1] + " " + address.split(" ")[2]
        zipcode = response.css("div.field-name-city-postal-code::text").get()
        available_date = response.css("span.date-display-single::text").get()

        location_script = response.css("script:contains('longitude')::text").get()
        latitude = re.findall("\"latitude\":\"(-?\d+\.\d+)\"", location_script)[0]
        longitude = re.findall("\"longitude\":\"(-?\d+\.\d+)\"", location_script)[0]

        landlord_name = "dorerentals"
        landlord_phone = "613-227-0000"
        landlord_email = "office@dorerentals.com"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("title", title)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)

        yield item_loader.load_item()
