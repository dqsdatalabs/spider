# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class FreedomImmobiliareSpider(Spider):
    name = 'freedomimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.freedomimmobiliare.it"]
    start_urls = ["https://www.freedomimmobiliare.it/ita/immobili?order_by=&company_id=&seo=&tabs=on&coords=&coords_center=&coords_zoom=&property_type_id=&property_subtype_id=&rental=1&city_id=&district_id=&price_max=&code="]

    def parse(self, response):
        site_pages = response.css("ul.pager li a::attr(href)").getall()
        first_page = site_pages[0].split("page=2")[0] + "page=1"
        site_pages.append(first_page)

        for url in site_pages:
            yield Request(response.urljoin(url), callback=self.populate_page, dont_filter = True)

    def populate_page(self, response):
        for page in response.css("div.property-title h4 a::attr(href)").getall(): 
            yield Request(response.urljoin(page), callback=self.populate_item, dont_filter=True)
    
    def populate_item(self, response):
            item_loader = ListingLoader(response=response)

            property_type = "apartment"
            rent = response.css("p.price::text").get()            
            title = response.css("h2.title span::text").get().strip()
            title = re.sub("-", "", title)
            if (
                ("commerciale" in title.lower()) 
                or ("ufficio" in title.lower()) 
                or ("magazzino" in title.lower()) 
                or ("box" in title.lower()) 
                or ("auto" in title.lower()) 
                or ("negozio" in title.lower()) 
                or ("vendita" in title.lower()) ):
                return
            square_meters = response.css("div.section:nth-child(1) > ul:nth-child(2) > li:nth-child(2) > b:nth-child(2)::text").get()
            room_count = response.css("div.section:nth-child(1) > ul:nth-child(2) > li:nth-child(3) > b:nth-child(2)::text").get()
            energy_label = response.css("span[title='Classe Energ.'] + b::text").get()
            description = response.css("p.description::text").get()
            external_id = response.css("h2.title span b::text").get().strip()

            images_to_add = []
            images = response.css("li div.sl::attr(style)").getall()
            for image in images:
                image_src = image.split("background-image: url(")[1].split(");")[0]
                images_to_add.append(image_src)

            script_text = response.css("#search-by-map > script:nth-child(3)::text").get()
            longitude = re.findall("mapCenterLng: ([0-9]+\.[0-9]+),", script_text)[0]
            latitude = re.findall("mapCenterLat: ([0-9]+\.[0-9]+),", script_text)[0]

            landlord_name = "freedomimmobiliare"
            landlord_email = "info@freedomimmobiliare.it"

            bathroom_count = response.css("span[title='Bagni'] + b::text").get()
            
            washing_machine = response.css("span[title='Lavatrice'] + b span::attr(class)").get()
            if(washing_machine):
                washing_machine = True
            else:
                washing_machine = False

            furnished = response.css("span[title='Arredato'] + b span::attr(class)").get()
            if(furnished):
                furnished = True
            else:
                furnished = False
            
            elevator = response.css("span[title='Ascensore'] + b span::attr(class)").get()
            if(elevator):
                elevator = True
            else:
                elevator = False

            balcony = response.css("span[title='Balcone/i'] + b span::attr(class)").get()
            if(balcony):
                balcony = True
            else:
                balcony = False
            
            parking = response.css("span[title='Parcheggio (Posti Auto)'] + b span::attr(class)").get()
            if(parking):
                parking = True
            else:
                parking = False
            
            address = title.split(" a ")[1]
            city = address.split(",")[0]
        
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("rent_string", rent)
            item_loader.add_value("title", title)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("description", description)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("images", images_to_add)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("parking", parking)
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("landlord_email", landlord_email)
        
            yield item_loader.load_item()
