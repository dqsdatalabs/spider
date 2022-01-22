# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class TheFisheyeViewSpider(Spider):
    name = 'fiesoli_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.fiesoli.it"]
    start_urls = ["https://www.fiesoli.it/ricerca-immobili.php?t=appartamento&in=affitto&mq-min=0&mq-max=1000&vani-min=0&vani-max=50&o=5&p=1"]

    def parse(self, response):
        for page in response.css("div.col-xl-6 article a::attr(href)").getall():
            yield Request(response.urljoin(page), callback=self.populate_item)
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = "apartment"
        rent = response.css("div.des-classic-price span::text").get()
        address = response.css("div.des h3::text").get()
        title = "Appartamento in Affitto  in zona " + address.lower()
        square_meters = response.css("li.lstno:nth-child(1)::text").get().strip().split(" ")[1]
        room_count = response.css("li:contains('Vani:')::text").get().strip().split("Vani: ")[1]
        images = response.css('div.row div.col-sm-6 a::attr(href)').getall()

        balcony = response.css("dl.list-terms-inline:nth-child(9) > dd:nth-child(2)::text").get()
        if int(balcony) > 0:
            balcony = True
        else:
            balcony = False

        city = "Florence"
        landlord_name = "fiesoli immobiliare"
        landlord_email = "info@fiesoli.it"
        landlord_phone = "055.2654579 - 055.2479499"

        description = response.css(".des > p:nth-child(3)::text").get()
        external_id = response.css(".breadcrumbs-custom-path > li:nth-child(3) > a:nth-child(1)::text").get().split(" ")[-1]
        
        latitude = None
        longitude = None
        try:
            iframe_src = response.css("iframe::attr(src)").get()
            latitude = iframe_src.split("lt=")[1].split("&")[0]
            longitude = iframe_src.split("lg=")[1]
        except:
            pass
        
        bathroom_count = response.css("li.lstno2:nth-child(4)::text").get().split("Servizi:")[1]

        floor = response.css("dt:contains('Piano:') + dd::text").get()

        parking = response.css("dt:contains('Posti auto') + dd::text").get()
        parking = int (parking)
        if(parking):
            parking = True
        else:
            parking = False
        
        energy_label = response.css("dt:contains('Classe Energetica (IPE):') + dd::text").get()
        energy_label = re.findall("([A-Z]{1})", energy_label)[0]
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("address", address)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("city", city)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("parking", parking)
        item_loader.add_value("floor", floor)
        if latitude:
            item_loader.add_value("latitude", latitude)
        if longitude:
            item_loader.add_value("longitude", longitude)

        yield item_loader.load_item()
