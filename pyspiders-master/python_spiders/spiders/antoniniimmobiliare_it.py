# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class AntoniniimmobiliareSpider(Spider):
    name = 'antoniniimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.antoniniimmobiliare.it"]
    start_urls = ["https://antoniniimmobiliare.it/en/properties/for-rent-rome"]

    def parse(self, response):
        for url in response.css("div.col-12 a.h-full::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        property_type = "apartment"
        title = response.css("h1.normal-case::text").get()
        rent = response.css("div.property__price::text").get().strip()
        if( not re.search("[0-9]+", rent)):
            return
        rent = "".join(rent.split("."))
        currency = "EUR"

        square_meters = re.findall("([0-9]+) sqm", title)
        if(len(square_meters)> 0):
            square_meters = square_meters[0]
        else: 
            square_meters = None

        city = response.css("div.property__locality::text").get()
        zipcode = response.css("div.property__postal-code::text").get()
        address = response.css("div.property__address::text").get()
        energy_label = response.css("span:contains('Energetic class') + span::text").get()
        images = response.css('.flex-col.w-full img::attr(data-src)').getall()

        landlord_name = "antoniniimmobiliare"
        landlord_phone = "+39 06 8414 630"
        landlord_email = "info@antoniniimmobiliare.it"

        script_location = response.css("script:contains('lng')::text").get()
        latitude = re.findall("lat=(-?[0-9]+\.[0-9]+)", script_location)[0]
        longitude = re.findall("lng=(-?[0-9]+\.[0-9]+)", script_location)[0]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("images", images)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)

        yield item_loader.load_item()
