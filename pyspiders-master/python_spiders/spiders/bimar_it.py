# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class BimarSpider(Spider):
    name = 'bimar_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.bimar.it"]
    start_urls = ["https://bimar.it/ricerca-annuncio?wpv-wpcf-numero-offerta=&wpv-tipo-di-contratto=affitto&wpv-tipo-di-immobile=0&wpv-regione=0&wpv-provincia=0&wpv-citta=0&wpv-sottozona=0&wpv-prezzo-vendita=0&wpv-prezzo-affitto=0&wpv-superficie=0&wpv-nro-locali=0&wpv_sort_orderby=post_date&wpv_sort_order=desc&wpv_filter_submit=CERCA"]

    def parse(self, response):
        site_pages = response.css("div.wpb_wrapper p a::attr(onclick)").getall()
        property_pages = []
        for page in site_pages:
            page = page.split("location.href = '")[1]
            page = page.split("';")[0]
            property_pages.append(page)
        
        for url in property_pages:
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a.wpv-filter-next-link::attr(href)").get()
        if next_page:
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):

        property_type = "apartment"
        title = response.css("h3 strong::text").get()
        title_lowered = title.lower()
        if (
            ("commerciale" in title_lowered) 
            or ("ufficio" in title_lowered) 
            or ("magazzino" in title_lowered) 
            or ("box" in title_lowered) 
            or ("auto" in title_lowered) 
            or ("negozio" in title_lowered) 
            or ("vendita" in title_lowered) ):
            return
        
        apartment_attributes = response.css("strong:contains('Locali')::text").get()

        room_count = re.findall("([0-9]+) Locali", apartment_attributes)[0]
        if(room_count == "0"):
            room_count = "1"

        bathroom_count = re.findall("([0-9]+) Bagni", apartment_attributes)[0]
        square_meters = re.findall("([0-9]+) mq", apartment_attributes)[0]
        rent = response.css("div.single-prezzo div.wpb_wrapper::text").get().strip()
        if( rent == ""):
            return
        currency = "EUR"

        images = response.css("noscript.justified-image-grid-html ul li a::attr(href)").getall()
        description = response.css("h5:contains('Descrizione') + p::text").get()
        external_id = response.css("div:contains('Riferimento') + div div div div div::text").get().strip()
        floor = response.css("div:contains('Piano') + div div div div div p::text").get()
        floor = re.findall("([0-9]+)", floor)[0]
        energy_label = response.css("div:contains('Classe Energetica') + div div div div div::text").get().strip()

        landlord_name = "bimar"
        landlord_phone = "800 07 33 28"
        landlord_email = "bimar@bimar.it"

        address = response.css("div.wpb_wrapper h3 + span::text").get()
        if(not address):
            address = response.css("div:contains('Zona') + div div div div div::text").get().strip()
            if(address.strip() == ""):
                address = response.css("div.single-descrizione:nth-child(5) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1)::text").get().strip()
        city = title.split("in affitto a")[1]

        lowerd_description = description.lower()
        balcony = "balcone" in lowerd_description
        dishwasher = "lavastoviglie" in lowerd_description
        washing_machine = "lavatrice" in lowerd_description
        furnished = "arredato" in lowerd_description

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("floor", floor)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("furnished", furnished)
       
        yield item_loader.load_item()
