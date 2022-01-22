# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class StellaImmobiliarepisaSpider(Spider):
    name = 'Stellaimmobiliarepisa_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.stellaimmobiliarepisa.it"]
    start_urls = ["https://www.stellaimmobiliarepisa.it/it/immobili?contratto=2&tipologia=&provincia=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&vani_min=&vani_max=&rif=&order_by=id&order_dir=desc"]

    def parse(self, response):
        for url in response.css("div.desc-box h4 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css("li.col-lg-4:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        title = response.css(".page-title > div:nth-child(1) > h2:nth-child(1)::text").get()
        square_meters = response.css("li.col-lg-4:nth-child(5) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        room_count = response.css(".slide-item-features > li:nth-child(3)::text").get().strip().split(" ")[0]
        bathroom_count = response.css(".slide-item-features > li:nth-child(2)::text").get().strip().split(" ")[0]
        
        images = response.css("img.base-image::attr(data-url)").getall()
        images_to_add = []
        for image in images:
            images_to_add.append(response.urljoin(image))
        
        description = response.css("p.first-paragraph::text").get()
        external_id = response.css("li.col-lg-4:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        
        furnished = response.css("li.col-lg-4:nth-child(13) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        furnished_found = re.search("arredato", furnished)
        if(not furnished_found):
            furnished = response.css("li.col-lg-4:nth-child(14) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        furnished_found = re.search("arredato", furnished)
        if(not furnished_found):
            furnished = response.css("li.col-lg-4:nth-child(12) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        furnished_found = re.search("arredato", furnished)
        if(not furnished_found):
            furnished = response.css("li.col-lg-4:nth-child(11) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        furnished_found = re.search("arredato", furnished)
        if(not furnished_found):
            furnished = response.css("li.col-lg-4:nth-child(10) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()
        furnished_found = re.search("arredato", furnished)
        if(not furnished_found):
            furnished = response.css("li.col-lg-4:nth-child(15) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)::text").get().strip()

        if(furnished == "arredato"):
            furnished = True
        else: 
            furnished = False

        landlord_name = "stellaimmobiliarepisa"
        landlord_phone = "+39 0507213782"
        landlord_email = "info@stellaimmobiliarepisa.it"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
