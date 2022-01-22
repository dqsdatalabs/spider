# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class AntonellianaSpider(Spider):
    name = 'antonelliana_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.antonelliana.it"]
    start_urls = ["https://www.antonelliana.it/status/affitto-residenziale/"]
    position = 1

    def parse(self, response):
        for url in response.css("a.hover-effect::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("ul.pagination li.page-item a[aria-label='Next']::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"
        title = response.css("div.page-title h1::text").get()
        rent = response.css("li.item-price::text").get()
        rent = rent.split("/")[0]
        rent = rent.split(".")
        rent = "".join(rent)
        currency = "EUR"
        room_count = response.css("i.icon-hotel-double-bed-1 + strong::text").get()
        bathroom_count = response.css("i.icon-bathroom-shower-1 + strong::text").get()
        square_meters = response.css("i.icon-real-estate-dimensions-plan-1 + strong::text").get() 
        images = response.css("div#lightbox-slider-js div img.img-fluid::attr(src)").getall()
        description = response.css("div#property-description-wrap div.block-wrap div.block-content-wrap p::text").getall()
        description = " ".join(description)
        utilities = response.css("strong:contains('SPESE CONDOMINIO:') + span::text").get()
        floor = response.css("strong:contains('PIANO:') + span::text").get()
        
        elevator = response.css("strong:contains('ASCENSORE:') + span::text").get()
        if(elevator):
            elevator = True

        parking = response.css("i.icon-car-1 + strong::text").get()
        
        if(parking):
            parking = True

        energy_label = response.css("strong:contains('CLASSE ENERGETICA:') + span::text").get()
        if( energy_label == None):
            energy_label = response.css("strong:contains('EFFICIENZA ENERGETICA:') + span::text").get()
        if(energy_label):
            energy_label = re.findall("([A-Z]) ", energy_label)
            if(len(energy_label) > 0):
                energy_label = energy_label[0]
            else:
                energy_label = None

        address = response.css("strong:contains('Indirizzo') + span::text").get()
        city = response.css("strong:contains('Città') + span::text").get()
        zipcode = response.css("strong:contains('CAP') + span::text").get()
        
        location_script = response.css("script#houzez-single-property-map-js-extra::text").get()
        latitude = re.findall('"lat":"(-?[0-9]+\.[0-9]+)"', location_script)[0]
        longitude = re.findall('"lng":"(-?[0-9]+\.[0-9]+)"', location_script)[0]
        external_id = re.findall('"property_id":"([0-9]+)"', location_script)[0]

        landlord_name = "antonelliana"
        landlord_phone = "011 596060"
        landlord_email = "info@antonelliana.it"
        
        floor_plan_images = response.css("div[data-parent='#floor-plans-1'] div.accordion-body a::attr(href)").get()
        terrace = None
        
        balcony = response.css("strong:contains('BALCONE') + span::text").get()
        if(balcony):
            if("terrazzo" in balcony.lower()):
                terrace = True
            else:
                terrace = False

            if("balcon" in balcony.lower()):
                balcony = True
            else:
                balcony = False

        furnished = response.css("strong:contains('ARREDAMENTO:') + span::text").get()
        if(furnished):
            if("arreda" in furnished.lower()):
                furnished = True
            else:
                furnished = False

        heating_cost = response.css("strong:contains('RISCALDAMENTO:') + span::text").get()
        heating_cost = re.findall("([0-9]+)", heating_cost)
        if(len(heating_cost) > 0):
            heating_cost = "".join(heating_cost)
        else:
            heating_cost = None

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position)
        self.position += 1
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("parking", parking)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("heating_cost", heating_cost)
       
        yield item_loader.load_item()
