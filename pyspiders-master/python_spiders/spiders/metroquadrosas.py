# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse
from python_spiders.loaders import ListingLoader
from python_spiders.helper import property_type_lookup, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates


class Metroquadrosas_PySpider_italy_it(scrapy.Spider):
    name = "metroquadrosas"
    start_urls = ['https://www.immobiliaremetroquadrosas.it/it/immobili?contratto=2&provincia=&prezzo_min=&prezzo_max=&camere_min=&camere_max=&rif=&order_by=&order_dir=']
    allowed_domains = ["immobiliaremetroquadrosas.it"]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    thousand_separator = '.'
    scale_separator = ',' 
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for listing in response.css(".griglia"):
            address = listing.css("a::text").get().strip()
            property_type = property_type_lookup.get(listing.css("address::text").get())
            path = listing.css("a::attr('href')").get()
            landlord_name = response.css(".pgl-copyrights p::text").get().split(" - ")[1]
            landlord_email = response.xpath("//a[@title='email']/text()").get()
            landlord_phone = response.css(".text-note::text").get()

            base_url = "{uri.scheme}://{uri.netloc}/".format(uri=urlparse(response.url))
            url = base_url + "it/" + path
            yield scrapy.Request(
                url,
                callback=self.populate_item,
                meta={
                    'address': address, 'property_type': property_type, "landlord_name": landlord_name,
                    'landlord_email': landlord_email, 'landlord_phone': landlord_phone,
                },
            )

        next_page = None
        if response.css(".pagination .active + li>a::text").get().isdigit():
            next_page = base_url + response.css(".pagination .active + li>a::attr('href')").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def populate_item(self, response):
        landlord_name = response.meta.get("landlord_name")
        landlord_email = response.meta.get("landlord_email")
        landlord_phone = response.meta.get("landlord_phone")
        property_type = response.meta.get("property_type")
        address = response.meta.get("address")
        city = address.split(", ")[-1]
        longitude, latitude = extract_location_from_address(address)
        zipcode, _, _ = extract_location_from_coordinates(longitude, latitude)

        title = response.css(".page-top-in span::text").get()
        images = response.css(".slides img::attr('src')").extract()
        description = response.css(".pgl-detail p::text").get()

        external_id = currency = floor = furnished = energy_label = parking = elevator = terrace = balcony = None
        rent = square_meters = room_count = bathroom_count = None
        for row in response.css(".amenities-detail li"):
            key, val = row.css("::text").extract()
            if "Prezzo" in key:
                rent, currency = extract_rent_currency(val, self.country, Metroquadrosas_PySpider_italy_it)
            elif "Rif" in key:
                external_id = val.strip()
            elif "Superficie" in key:
                square_meters = int(val.strip().split(" ")[0])
            elif "Piano" in key:
                floor = val.strip()
            elif "Balconi" in key:
                balcony = val.strip() == "sì"
            elif "Camere totali" in key:
                room_count = int(val)
            elif "Bagni" in key:
                bathroom_count = int(val)
            elif "Arredato" in key:
                furnished = val.strip().lower() == "arredato"
            elif "Classe energ" in key and len(val) < 3:
                energy_label = val.strip()
            elif "Ascensore" in key:
                elevator = val.strip() == "sì"
            elif "Posti auto" in key:
                parking = True
            elif "Terrazzi" in key:
                terrace = val.strip() == "sì"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)

        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)

        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
