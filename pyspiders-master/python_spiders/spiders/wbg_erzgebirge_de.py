# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Wbg_erzgebirge_deSpider(Spider):
    name = 'wbg_erzgebirge_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.wbg-erzgebirge.de"]
    start_urls = ["https://www.wbg-erzgebirge.de/wohnungen/wohnungssuche/?zvon=1&zbis=6&mvon=150&mbis=975&fvon=30&fbis=130"]
    position = 1

    def parse(self, response):
        for url in response.css("div.wss-details-button a.btn-primary:contains('Mehr Anzeigen')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a:contains('weiter')::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1::text").get()
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

        rent = response.css("div.ktd-text:contains('Kaltmiete') + div.ktd-wert::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        utilities = response.css("div.ktd-text:contains('Nebenkosten') + div.ktd-wert::text").get()
        if( utilities):
            utilities = utilities.split(",")[0]
        
        room_count = response.css("div.dtc-text:contains('Wohnräume:') + div.dtc-content::text").get()
        floor = response.css("div.dtc-text:contains('Wohngeschoss:') + div.dtc-content::text").get()
        available_date = response.css("div.dtc-text:contains('Bezugsfertig ab:') + div.dtc-content::text").get()

        features = response.css("div.itd-text::text").getall()
        features = " ".join(features)

        balcony = "Balkon" in features
        images = response.css("ul.imgslider img::attr(src)").getall()
        floor_plan_images = response.css("div#wohnung-grundriss img::attr(src)").getall()

        location_data = extract_location_from_address(title)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        landlord_name = "wbg-erzgebirge"
        landlord_phone = "03733 5698-0"
        landlord_email = "info@wbg-erzgebirge.de"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 

        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) 
        item_loader.add_value("floor", floor) 
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("available_date", available_date)  

        item_loader.add_value("balcony", balcony) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 
        item_loader.add_value("floor_plan_images", floor_plan_images) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
