# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from scrapy.http import FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, property_type_lookup, remove_white_spaces, currency_parser, extract_location_from_address, extract_location_from_coordinates
from urllib.parse import urlparse
import requests


numbers = {
    "uno": 1,
    "due": 2,
    "tre": 3,
    "quattro": 4,
    "cinque": 5,
    "sei": 6,
    "sette": 7,
    "otto": 8,
    "nove": 9,
    "dieci": 10,
    "undici": 11,
    "dodici": 12,
}

class SilStudio_PySpider_italy_it(scrapy.Spider):
    name = "sil_studio_it"
    allowed_domains = ["sil.it"]
    start_urls = ["https://www.sil.it/immobili/immobili_in_affitto.html"]
    execution_type = "testing"
    country = "italy"
    locale = "it"
    thousand_separator = ','
    scale_separator = '.'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        page = response.meta.get("page", 1)
        for unit in response.css(".real-estate-item"):
            title = unit.css(".real-estate-item-desc a::text").get()
            if "commerciale" in title or "ufficio" in title:
                continue
            url = unit.css(".real-estate-item-image a::attr('href')").get()
            base_url = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(response.request.url))
            yield scrapy.Request(base_url + url, callback=self.populate_item)

        if len(response.css(".pag").css("span + a").extract()) > 0:
            page += 1
            yield FormRequest(url=response.request.url, method="POST", formdata={"num_page": str(page)}, meta={"page": page}, callback=self.parse)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        square_meters = int(extract_number_only(response.css(".f-mq strong::text").get()))
        property_type = property_type_lookup.get(response.css(".f-tipologia > h5::text").get(), "")
        rent = int(extract_number_only(response.css("#det_prezzo::attr('data-valore')").get()))
        room_count = int(extract_number_only(response.css(".f-camere strong::text").get()))

        if not property_type or not square_meters or not room_count or not rent:
            return

        bathroom_count = int(extract_number_only(response.css(".f-bagni strong::text").get()))
        external_id = response.css(".f-rif strong::text").get().strip()
        city = response.css("#det_prov::attr('data-valore')").get().strip()
        title = response.css(".heading-block > h1::text").get().strip()
        address = response.css(".sottotitolo::text").get().strip()
        parking = response.css(".f-garage strong::text").get() is not None
        elevator = response.css(".f-ascensore strong::text").get() is not None
        currency = currency_parser(response.css(".real-estate-item-price3 > h3::text").get(), self.country)
        floor = str(extract_number_only(response.css("#det_piano::attr('data-valore')").get()))
        images = list(set(response.css(".sp-thumbnail::attr('data-src')").extract()))
        floor_plan_images = list(set(response.css(".image_fade::attr('src')").extract()))
        furnished = response.css("#det_arredato .valore::text").get()
        energy_label = response.css("#det_cl_en::attr('data-valore')").get()
        terrace = True if response.css("#det_terrazza") else False
        landlord_phone = response.css(".agency_telephone a::attr('href')").get()
        landlord_email = response.css(".agency_email a::attr('href')").get()
        landlord_name = response.css(".nomeagenzia::text").get()

        description_lines = [remove_white_spaces(x) for x in response.css("#sez-descrizione::text").extract()]
        description = "".join([line for line in description_lines if line])

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", f"SilStudio_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent", rent)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("address", address)

        if parking:
            item_loader.add_value("parking", parking)
        if elevator:
            item_loader.add_value("elevator", elevator)
        if title:
            item_loader.add_value("title", title)
        if furnished:
            item_loader.add_value("furnished", furnished.lower() == "si")
        if energy_label and len(energy_label) <= 2:
            item_loader.add_value("energy_label", energy_label)
        if terrace:
            item_loader.add_value("terrace", terrace)
        if landlord_phone.startswith("tel"):
            item_loader.add_value("landlord_phone", landlord_phone[4:])
        if landlord_email.startswith("mailto"):
            item_loader.add_value("landlord_email", landlord_email[7:])
        if len(images) > 0:
            item_loader.add_value("images", images)
        if len(floor_plan_images) > 0:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        if currency:
            item_loader.add_value("currency", currency)
        else:
            item_loader.add_value("currency", "EUR")

        for section in response.css(".gruppo"):
            header = section.css(".nomegruppo strong::text")
            if not header:
                continue
            if "DEPOSITO" in header.get():
                months = numbers.get(section.css(".accessorio > strong::text").get().split(" ")[0].lower(), 0)
                if months:
                    item_loader.add_value("deposit", months * int(rent))
        
        latitude = longitude = ""
        map_url = response.xpath("//iframe[@title='Mappa']/@src").get()
        if map_url:
            parsed = urlparse(map_url)
            for param in parsed.query.split("&"):
                if param.startswith("q="):
                    latitude, longitude = param[2:].split(",")
                    break

        if not latitude or not longitude:
            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)

        zipcode, _, _ = extract_location_from_coordinates(longitude, latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("zipcode", zipcode)

        self.position += 1

        yield item_loader.load_item()
