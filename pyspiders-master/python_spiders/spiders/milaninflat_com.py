# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class MilaninFlatSpider(Spider):
    name = 'milaninflat_com'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.milaninflat.com/"]
    start_urls = ["https://www.milaninflat.com/appartamenti-affitto-breve-milano"]

    def parse(self, response):
        for url in response.css("div.item div.page-header h2 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css(".icone > div:nth-child(3)::text").get().strip()
        currency = "EUR"
        address = response.css("div.page-header div.subTitle::text").get().strip()
        title = response.css("div.page-header h1::text").get().strip()
        square_meters = response.css(".icone > div:nth-child(1)::text").get().strip()
        room_count = response.css("#ospit::text").get().strip()

        images = response.css("li.itm img::attr(src)").getall()
        images_to_add = []
        for image in images:
            images_to_add.append(self.allowed_domains[0] + image)
        
        description = response.css("div#descrizione div p::text").getall()
        description = " ".join(description)
        
        location_script_text = response.css(".contenuto > script:nth-child(4)::text").get()
        longitude = re.findall("{center:{lat:[0-9]+\.[0-9]+,lng:([0-9]+\.[0-9]+)}", location_script_text)
        latitude = re.findall("{center:{lat:([0-9]+\.[0-9]+),lng:[0-9]+\.[0-9]+}", location_script_text)

        landlord_phone = "+39 347.2798989"
        landlord_email = "info@milaninflat.com"
        landlord_name = "milanflat"
        city = "Milano"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("description", description)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("city", city)
       
        yield item_loader.load_item()
