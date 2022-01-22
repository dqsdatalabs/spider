# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re 

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class CastellanoImmobiliareSpider(Spider):
    name = 'castellanoimmobiliare_com'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.castellanoimmobiliare.com"]
    start_urls = ["https://castellanoimmobiliare.com/it/affitto"]

    def parse(self, response):
        for url in response.css("div.estate-wrapper-content a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.css("div.intro-estate::text").get().strip()
        title_lowered = title.lower()
        if (
            ("commerciale" in title_lowered) 
            or ("ufficio" in title_lowered) 
            or ("magazzino" in title_lowered) 
            or ("box" in title_lowered) 
            or ("auto" in title_lowered) 
            or ("negozi" in title_lowered) 
            or ("terr") in title_lowered
            or ("vendita" in title_lowered) ):
            return
        property_type = "apartment"

        rent = response.css(".prezzo::text").get()
        
        square_meters = response.css("div.wrap:nth-child(2) > div:nth-child(2) > div:nth-child(2)::text").get()
        room_count = response.css("div.wrap:nth-child(2) > div:nth-child(5) > div:nth-child(2)::text").get()
        energy_label = response.css("div.wrapper_class_energetic span::text").get()
        bathroom_count = response.css("div.flex:nth-child(5) > div:nth-child(2)::text").get()
        description = response.css("#ContentEstate > p:nth-child(2)::text").get()
        
        images = response.css("figure a img::attr(data-src)").getall()
        images_to_add = []
        for image in images:
            images_to_add.append(response.urljoin(image))
    
        longitude = response.css("span.address::attr(data-longitude)").get()
        latitude = response.css("span.address::attr(data-latitude)").get()

        elevator = response.css("div.flex:nth-child(6) > div:nth-child(2)::text").get()
        if(elevator == "No"):
            elevator = False
        else:
            elevator = True
        landlord_email = "info@castellanoimmobiliare.com"
        landlord_name = "castellanoimmobiliare"
        landlord_phone = "+39 081 807 15 33"

        external_id = response.css("span.code::text").get()
        city = response.css(".details > div:nth-child(1) > div:nth-child(2)::text").get()
        address = city

        floor_plan_images = images_to_add

        floor = response.css("div.wrap:nth-child(2) > div:nth-child(4) > div:nth-child(2)::text").get()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("description", description)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("address", address)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)
        item_loader.add_value("floor_plan_images", floor_plan_images)
       
        yield item_loader.load_item()
