# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class FlorenceRealEstateSpider(Spider):
    name = 'florencerealestate_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.florencerealestate.it"]
    start_urls = ["https://www.florencerealestate.it/affitti/"]

    def parse(self, response):
        for url in response.css("h2.entry-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        title = response.css("header h1.entry-title::text").get()
        if("in" not in title and "affitto" in title.lower()):
            city = title.lower().split("affitto")[1]

        if("in" in title):
            city = title.split("in")[1]

        if( "affitto" in city):
            city = city.split("affitto")[1]

        rent = response.css(".container_label > ul:nth-child(1) > li:nth-child(2) > a:nth-child(1) > span:nth-child(1)::text").get()
        square_meters = response.css(".container_label > ul:nth-child(1) > li:nth-child(1) > a:nth-child(1) > span:nth-child(1)::text").get()
        images = response.css("img.attachment-thumbnail::attr(src)").getall()
        energy_label = response.css(".container_label > ul:nth-child(1) > li:nth-child(3) > a:nth-child(1) > span:nth-child(1)::text").get()
        description = response.css(".entry > p:nth-child(3)::text").get()
        landlord_email = "florencerealestate69@gmail.com"
        landlord_name = "Florence Real Estate"
        landlord_phone = "0552342206"
        try: 
            address = title.split("in")[1]
        except IndexError:
            address = title.split("Affitto")[1]
        external_id = response.css("link[rel='shortlink']::attr(href)").get().split("?p=")[1]

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("city", city)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("address", address)
        item_loader.add_value("external_id", external_id)
       
        yield item_loader.load_item()
