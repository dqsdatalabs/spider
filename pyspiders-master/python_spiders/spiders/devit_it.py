# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class AssociatedpmSpider(Spider):
    name = 'devit_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.devit.it"]
    start_urls = ["https://www.devit.it/site/affitto/"]

    def parse(self, response):
        for url in response.css("a.listing-featured-thumb::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):

        property_type = "apartment"

        title = response.css("div.page-title h1::text").get()
        if(title):
            lowered_title = title.lower()
            if (
                ("commerciale" in lowered_title) 
                or ("ufficio" in lowered_title) 
                or ("magazzino" in lowered_title) 
                or ("box" in lowered_title) 
                or ("auto" in lowered_title) 
                or ("negozio" in lowered_title) 
                or ("vendita" in lowered_title) ):
                return

        rent = response.css("li.item-price::text").get()
        currency = "EUR"

        address = response.css("address.item-address::text").get()
        city = response.css("strong:contains('Città') + span::text").get()
        description = response.css("div#property-description-wrap div.block-wrap div.block-content-wrap p::text").getall()
        description = " ".join(description)

        external_id = response.css("strong:contains('ID Proprietà:') + span::text").get()

        square_meters = response.css("strong:contains('SUPERFICIE:') + span::text").get()
        if( not square_meters):
            square_meters = response.css("strong:contains('Area:') + span::text").get()
        room_count = response.css("strong:contains('Camere da letto:') + span::text").get()
        bathroom_count = response.css("strong:contains('Bagn') + span::text").get()
        images = response.css("a.houzez-trigger-popup-slider-js img::attr(src)").getall()
        floor = response.css("strong:contains('TOTALE PIANI EDIFICIO:') + span::text").get()

        energy_label = response.css("strong:contains('EFFICIENZA ENERGETICA:') + span::text").get()
        if(not energy_label):
            energy_label = response.css("strong:contains('Certificazione Energetica:') + span::text").get()
        if(energy_label):
            energy_label = str(energy_label)
            energy_label = energy_label.split("≥")[0]

        features = response.css("ul.list-unstyled li a::text").getall()
        features = " ".join(features).lower()

        elevator = "ascensore" in features
        balcony = "balcone" in features
        furnished = "non arredato" not in features

        location_script = response.css("script#houzez-single-property-map-js-extra::text").get()
        
        latitude = re.findall('"lat":"(-?[0-9]+\.[0-9]+)"', location_script)[0]
        longitude = re.findall('"lng":"(-?[0-9]+\.[0-9]+)"', location_script)[0]

        landlord_name = 'devit'
        landlord_phone = "081 66 15 36"
        landlord_email = "info@devit.it"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("floor", floor)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
