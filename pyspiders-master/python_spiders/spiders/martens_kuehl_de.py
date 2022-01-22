# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import urllib
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Martens_kuehl_deSpider(Spider):
    name = 'martens_kuehl_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.martens-kuehl.de"]
    start_urls = [
        "https://www.martens-kuehl.de/immobilien/vermietung/wohnimmobilien/",
        "https://www.martens-kuehl.de/immobilien/vermietung/neubauwohnungen/"
        ]
    position = 1

    def parse(self, response):
        ads = response.css("div.caption")
        for ad in ads:
            reserved = ad.css("div:contains('reserviert')::text").getall()
            reserved = "".join(reserved).strip()
            reserved = re.sub(r"\s+", " ", reserved)
            url = ad.css("a.btn-default::attr(href)").get()
            if("reserviert" not in reserved):
                yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)      

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("article h3::text").get()
        lowered_title = title.lower()
        if(
            "gewerbe" in lowered_title
            or "gewerbefläche" in lowered_title
            or "büro" in lowered_title
            or "praxisflächen" in lowered_title
            or "ladenlokal" in lowered_title
            or "arbeiten" in lowered_title 
            or "gewerbeeinheit" in lowered_title
            or "vermietet" in lowered_title
            or "stellplatz" in lowered_title
            or "garage" in lowered_title
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
        ):
            return

        rent = response.css("th:contains('Kaltmiete:') + td::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        utilities = response.css("th:contains('Nebenkosten:') + td::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]

        deposit = response.css("th:contains('Mietkaution:') + td::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]
        
        energy_label = response.css("th:contains('Klasse:') + td::text").get()
        external_id = response.css("h4:contains('Objekt Nr.')::text").get()
        external_id = external_id.split(" Nr. ")[1]
        
        city = response.css("th:contains('Standort:') + td::text").get()
        floor = response.css("th:contains('Etage:') + td::text").get()
        living_space = response.css("th:contains('Wohnfläche:') + td::text").get()

        room_count = re.findall(r" (\d{1}) Zimmer", living_space)[0]
        square_meters = living_space.split(" m²")[0]
        square_meters = re.findall(r"([0-9]+)", living_space)
        square_meters = ".".join([square_meters[0], square_meters[1]])
        square_meters = math.ceil(float(square_meters))

        images = response.css("img.rsTmb::attr(src)").getall()
        images = [response.urljoin(urllib.parse.quote(image_src)) for image_src in images]

        contacts = response.css("div.detail *::text").getall()
        landlord_name = contacts[1]
        landlord_phone = contacts[4]
        landlord_email = contacts[6]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 

        item_loader.add_value("city", city) 

        item_loader.add_value("floor", floor) 
        item_loader.add_value("property_type", property_type)  
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
