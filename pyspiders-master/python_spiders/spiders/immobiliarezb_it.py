# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re 
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

from ..helper import *

class ImmobiliarezbSpider(Spider):
    name = 'Immobiliarezb_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.immobiliarezb.it"]
    start_urls = ["https://www.immobiliarezb.it/easyStore/index.asp?SettoreID=002&MaxMq=&MaxPrice=&strCerca=&bolCerca=1"]

    def parse(self, response):
        for url in response.css("div.box a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css("div#bgprezzo span.prezzo::text").get().strip()
        title = response.css(".breadcrumbs > em:nth-child(5)::text").get()
        if (
            ("commerciale" in title.lower()) 
            or ("ufficio" in title.lower()) 
            or ("magazzino" in title.lower()) 
            or ("box" in title.lower()) 
            or ("auto" in title.lower()) 
            or ("negozio" in title.lower()) 
            or ("vendita" in title.lower()) ):
            return

        square_meters = response.css("div.fullrow:nth-child(2)::text").get().strip()
        room_count = response.css(".contcustom > div:nth-child(4) > span:nth-child(2)::text").get().strip()
        
        images_to_add = []
        images = response.css("ul.slides li img::attr(src)").getall()
        for image in images:
            image = re.sub("min", "zoom", image)
            images_to_add.append(f"https://{self.allowed_domains[0]}{image}")

        landlord_name = "immobiliarezb"
        landlord_phone = "+39 010 3106620"
        landlord_email = "info@immobiliarezb.it"

        elevator = response.css("label:contains('Ascensore') + span::text").get()

        if( elevator ):
            elevator = True
        else:
            elevator = False

        floor = response.css("div.fullrow:nth-child(3) > span:nth-child(2)::text").get()
        balcony = response.css("div.fullrow:nth-child(6) > label:nth-child(1)::text").get()
        if(balcony == "Spazio esterno"):
            balcony = True
        else:
            balcony = False

        description = response.css("div#colonnasx div.fullrow p span::text").getall()
        description = " ".join(description)
        description = re.sub("(<.+>)", "", description)
        city = response.css("span.piccolo::text").get()
        address = title.split(",")[0]
        address = f"{city} {address}"

        iframe_src = response.css("iframe::attr(src)").get()
        iframe_response = requests.get(response.urljoin(iframe_src))
        map_iframe_returned = iframe_response.text
        location = re.findall("new google.maps.LatLng\((\d+.\d+,\d+.\d+)\);", map_iframe_returned)[0]
        
        latitude = location.split(",")[0]
        longitude = location.split(",")[1]

        utilities = response.css("label:contains('Spese condominiali') + span::text").get()
        energy_label = response.css("label:contains('Classe energetica') + span::text").get()
        bathroom_count = response.css("label:contains('Bagni') + span::text").get()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("room_count", int(room_count))
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("description", description)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("floor", floor)
       
        yield item_loader.load_item()
