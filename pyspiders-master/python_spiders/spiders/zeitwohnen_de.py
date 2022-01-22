# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class ZeitwohnenSpider(Spider):
    name = 'zeitwohnen_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.zeitwohnen.de"]
    start_urls = ["https://www.zeitwohnen.de/suchergebnisse/"]
    position = 1

    def parse(self, response):
        for url in response.css("div.property_listing h4 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("li.roundright a::attr(href)").get()
        if (next_page != response.url and next_page != None):
            yield response.follow(response.urljoin(next_page), callback=self.parse)

    def populate_item(self, response):
        
        property_type = "apartment"
        title = response.css("h1.entry-title::text").get()
        rent = response.css("span.price_area::text").get()
        if(not re.search("([0-9]*\.?[0-9]*)", rent)):
            return

        rent = re.findall("Nebenkosten: ([0-9]*\.?[0-9]*) € pro", rent)
        rent = "".join(rent)

        if("." in rent):
            rent = rent.split(".")
            rent = "".join(rent[0:2])
        rent = rent.strip()
        if(rent == ""):
            return
        currency = "EUR"

        images = response.css("div.item img::attr(src)").getall()

        zipcode = response.css("div.listing_detail:contains('PLZ:')::text").get().strip()
        city = response.css("div.listing_detail:contains('Ort:')::text").get()

        address = f"{zipcode}, {city}"

        external_id = response.css("div.listing_detail:contains('Objekt-ID:')::text").get().strip()
        square_meters = response.css("div.listing_detail:contains('Größe:')::text").get()
        room_count = response.css("div.listing_detail:contains('Zimmer:')::text").get()
        floor = response.css("div.listing_detail:contains('Etage:')::text").get()
        available_date = response.css("div.listing_detail:contains('Bezugsfrei ab:')::text").get()

        description = response.css("div.wpestate_property_description p::text").getall()
        description = " ".join(description)
        
        amenities = response.css("div.listing_detail::text").getall()
        amenities = " ".join(amenities)

        furnished = "vollmöbliert" in amenities or "vollmöbliert" in description
        parking = "Park-/Stellplatz vorhanden" in amenities or "Park-/Stellplatz vorhanden" in description
        terrace = "Terrasse" in amenities or "Terrasse" in description
        washing_machine = "Waschmaschine" in amenities or "Waschmaschine" in description

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
        latitude = str(latitude)

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        longitude = str(longitude)

        landlord_name = "zeitwohnen"
        landlord_phone = "+49 (0) 221 – 800 23 40"
        landlord_email = "info@zeitwohnen.de"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position)
        self.position += 1
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("images", images)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("floor", floor)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("description", description)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
