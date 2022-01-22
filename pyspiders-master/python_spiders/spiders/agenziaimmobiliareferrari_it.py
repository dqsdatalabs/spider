# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import urllib.parse

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class AgenziaImmobiliareFerrariSpider(Spider):
    name = 'Agenziaimmobiliareferrari_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.agenziaimmobiliareferrari.it"]
    start_urls = ["https://agenziaimmobiliareferrari.it/site/residenziale-affitto.html"]

    def parse(self, response):
        urls = {}
        for url in response.css("div.property a::attr(href)").getall():
            urls[url] = url
        for url in urls:
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"

        address = response.css("div.well:nth-child(2) > h4:nth-child(1) > span:nth-child(2)::text").get()
        title = response.css("div.alert h4::text").get()
        square_meters = response.css("#tab-1 table tr:nth-child(5) th::text").get()
        room_count = response.css("#tab-1 table tr:nth-child(7) th::text").get()
        bathroom_count = response.css("#tab-1 table tr:nth-child(9) th::text").get()
        
        rent = response.css("#tab-1 table tr:nth-child(3) th::text").get()
        if( not rent ):
            rent = response.css("#tab-1 table tr:nth-child(4) th::text").get()
            square_meters = response.css("#tab-1 table tr:nth-child(7) th::text").get()
            room_count = response.css("#tab-1 table tr:nth-child(9) th::text").get()
            bathroom_count = response.css("#tab-1 table tr:nth-child(11) th::text").get()
        
        try:
            int(square_meters)
        except:
            square_meters = response.css("#tab-1 table tr:nth-child(6) th::text").get()
            room_count = response.css("#tab-1 table tr:nth-child(8) th::text").get()
            bathroom_count = response.css("#tab-1 table tr:nth-child(10) th::text").get()

        try:
            int(square_meters)
        except:
            square_meters = response.css("#tab-1 table tr:nth-child(7) th::text").get()
            room_count = response.css("#tab-1 table tr:nth-child(9) th::text").get()
            bathroom_count = response.css("#tab-1 table tr:nth-child(11) th::text").get()

        imgaes_to_add = []
        images = response.css("div img::attr(src)").getall()
        
        for image in images:
            imgaes_to_add.append(response.urljoin(urllib.parse.quote(image)))

        
        landlord_email = "info@agenziaimmobiliareferrari.it"
        landlord_name = "Agenzia Immobiliare Ferrari"
        landlord_phone = "02 55181322-68"
        
        city = address.split(" ")[-1]
        
        description = response.css("p::text").getall()
        description = " ".join(description)
        
        furnished = None
        composition_data = response.css("th.text-right::text").getall()
        for line in composition_data:
            if("Arredato" in line.strip()):
                if("Non" in line.strip()):
                    furnished = False
                else:
                    furnished = True

        map_script = response.xpath("/html/body/script[11]/text()").get()
        map_script = map_script.split("\n")
        longitude = None
        latitude = None
        for line in map_script:
            line = line.strip()
            if("lat" in line):
                latitude = line.split(":")[1].strip().split(",")[0]
            if("lng" in line):
                longitude = line.split(":")[1].strip()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("address", address)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", imgaes_to_add)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("city", city)
        item_loader.add_value("description", description)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        if furnished:
            item_loader.add_value("furnished", furnished)

       
        yield item_loader.load_item()
