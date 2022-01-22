# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, format_date, extract_location_from_coordinates


class Carl_bethge_PySpider_germany_de(scrapy.Spider):
    name = "carl_bethge"
    start_urls = ['https://www.carl-bethge.de/mietobjekte/mietwohnungen/', 'https://www.carl-bethge.de/mietobjekte/haeuser/']
    allowed_domains = ["carl-bethge.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        self.landlord_name = response.css("#footer:nth-child(1) p::text").get()
        for row in response.css("#footer:nth-child(1) p"):
            if "Tel" in row.css("::text").get(""):
                self.landlord_phone = row.css("::text").get("").replace("Tel.", "").strip()
            if "mail" in row.css("::text").get("").lower():
                mail_items = [x.strip() for x in row.css("::text").extract() if x.strip()]
                self.landlord_email = "".join(mail_items[-2:])
        for url in response.css(".objekte-item-desc a::attr('href')"):
            yield scrapy.Request(url.get(), callback=self.populate_item)

    def populate_item(self, response):
        property_type = "apartment" if "wohnung" in response.css("title::text").get() else "house"
        address = city = latitude = longitude = zipcode = None
        address_items = response.css(".objekte-detail-header .box-content *::text").extract()
        if address_items:
            if "Balkon" in address_items[0]:
                city = "Bremerhaven"
                address = " ".join([city] + address_items[1:])
            else:
                address = " ".join(address_items)
                city = address_items[0].split("-")[0]
            longitude, latitude = extract_location_from_address(address)
            zipcode, _, _ = extract_location_from_coordinates(longitude, latitude)

        external_id = room_count = square_meters = floor = available_date = rent = utilities = deposit = heating_cost = None
        for row in response.css(".objekte-detail-content-table tr"):
            key_val = [line.strip() for line in row.css("td *::text").extract() if line.strip()]
            if len(key_val) == 2:
                key, val = key_val
            elif len(key_val) == 4:
                key = " ".join(key_val[:2])
                val = " ".join(key_val[:4]).replace("\n", " ")
            else:
                continue

            if "Objekt-Nr" in key:
                external_id = val
            elif "Zimmer" in key:
                room_count = int(float(val.replace(",", ".")))
            elif "Größe" in key:
                square_meters = round(float(val.split(" ")[0].replace(",", ".")))
            elif "Etage" in key:
                floor = val
            elif "frei ab" in key and "sofort" not in val:
                pattern = "%d.%m.%y" if len(val.split(".")[-1]) == 2 else "%d.%m.%Y"
                available_date = format_date(val, pattern)
            elif "Preis" in key:
                rent = int(float(val.split(" ")[0].replace(",", ".")))
            elif "Nebenkosten" in key:
                utilities = int(val.split(",")[0].split(":")[-1])
            elif "Mietsicherheit" in key:
                deposit = int(float(val.split(" ")[0].replace(",", ".")))
            elif "Heizung" in key:
                heating_cost = int(val.split(",")[0])

        if rent is None:
            return

        balcony = terrace = elevator = None
        for row in response.css(".objekte-detail-content-list li::text").extract():
            if "Balkon" in row:
                balcony = True
            elif "Terrasse" in row:
                terrace = True
            elif "Aufzug" in row:
                elevator = True

        images = response.css(".objekte-detail-gallery img::attr('src')").extract()
        description = "\r\n".join([x.strip() for x in response.css(".objekte-detail-content-text::text").extract()[:2] if x.strip() and "Uhr" not in x])

        sanitized_desc = description.lower()
        bathroom_count = washing_machine = None
        if "bad" in sanitized_desc:
            bathroom_count = 1
        if "waschmaschine" in sanitized_desc:
            washing_machine = True

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", address)

        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("available_date", available_date)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("heating_cost", heating_cost)

        item_loader.add_value("landlord_name", self.landlord_name)
        item_loader.add_value("landlord_phone", self.landlord_phone)
        item_loader.add_value("landlord_email", self.landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
