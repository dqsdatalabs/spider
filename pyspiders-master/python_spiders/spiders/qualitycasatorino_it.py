# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class TheFisheyeViewSpider(Spider):
    name = 'qualitycasatorino_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.qualitycasatorino.it"]
    start_urls = ["https://www.qualitycasatorino.it/immobili/ricerca/contratto_2/1"]

    def parse(self, response):
        site_pages = response.css("ul.pagination li a::attr(href)").getall()
        first_page = site_pages[1].split("/2")[0] + "/1"
        site_pages[0] = first_page

        for url in site_pages:
            yield Request(response.urljoin(url), callback=self.populate_page)

    def populate_page(self, response):
        property_pages_and_id = {}
        property_pages = response.css("div.imgHolder a::attr(href)").getall()
        property_list_li = response.css("div.postColumnFoot ul.list-unstyled li strong.fwNormal::text").getall()
        for page in property_pages: 
            yield Request(response.urljoin(page), callback=self.populate_item)
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css("div.col-xs-8:nth-child(1) > h2:nth-child(1)::text").get()
        if (("commerciale" in title.lower()) 
        or ("ufficio" in title.lower()) 
        or ("magazzino" in title.lower())):
            return

        availability = response.css("div.col-sm-6:nth-child(1) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(7) > td:nth-child(2)::text").get()
        if( "occupato" in availability.lower()):
            return

        property_type = "apartment"
        rent = response.css("h2.text-right::text").get()
        square_meters = response.css("div.col-sm-6:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(2)::text").get()
        room_count = response.css("div.col-sm-6:nth-child(1) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(2)::text").get()
        bathroom_count = response.css("div.col-sm-6:nth-child(1) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(4) > td:nth-child(2)::text").get()
        address = response.css("div.col-xs-8:nth-child(3) > p:nth-child(1)::text").get()
        images = response.css("div > img::attr(src)").getall()

        landlord_name = response.css("h2.fontNeuron:nth-child(2)::text").get()
        landlord_phone = response.css(".hb-numberbox > h3:nth-child(1)::text").get()
        landlord_email = "info@qualitycasatorino.it"
        city = address.split(",")[1]
        description = response.css("#descrizione > p:nth-child(2)::text").get()
        external_id = re.findall( "[A-Z]+[0-9]+", description)[0]
        latitude = response.css("input[id='property_latitude']::attr(value)").get()
        longitude = response.css("input[id='property_longitude']::attr(value)").get()
        utilities = response.css("div.col-sm-6:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(7) > td:nth-child(2)::text").get()

        zipcode = address.split("TO ")[1].strip()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("zipcode", zipcode)
       
        yield item_loader.load_item()

