# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_date, extract_location_from_address, extract_location_from_coordinates


class Immo_jb_PySpider_germany_de(scrapy.Spider):
    name = "immo_jb"
    start_urls = ['https://immo-jb.de/immobilien/immobilien-mieten']
    allowed_domains = ["immo-jb.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css("body > .color-every-second-row"):
            if listing.css(".status-red").get():
                continue
            if "stellplatz" in listing.css("h1::text").get("").lower():
                continue
            url = listing.css(".green-button::attr('href')").get()
            yield response.follow(url, callback=self.populate_item)

    def populate_item(self, response):
        title = response.css("#content h1::text").get()
        property_type = None
        if "wohnung" in response.url:
            property_type = "apartment"
        elif "haus" in response.url:
            property_type = "house"

        city = address = floor = available_date = elevator = None
        square_meters = room_count = bathroom_count = utilities = deposit = None
        for row in response.css(".immobilien-uebersicht2 tr"):
            key, val = [item.strip() for item in row.css("td::text").extract()]
            if "Wohnfläche" in key:
                square_meters = int(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = int(float(extract_number_only(val)))
            elif "Bäder" in key:
                bathroom_count = 1
            elif "Miete" in key:
                rent = int(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = int(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = int(float(extract_number_only(val)))
            elif "Ort" in key:
                city = val
            elif "Straße" in key:
                address = val
            elif "Etage" in key:
                floor = val
            elif "Bezugsfrei" in key:
                available_date = extract_date(val)
            elif "aufzug" in key:
                elevator = val != "Nein"

        if city and address:
            address += ", " + city

        longitude, latitude = extract_location_from_address(address)
        zipcode, _, _ = extract_location_from_coordinates(longitude, latitude)
        floor_plan_images = [response.urljoin(response.css("#content .big_green_link_list a::attr('href')").get())]
        images = response.css(".objektansicht::attr('href')").extract()

        landlord_name = response.css("title::text").get("").split("|").pop().strip()
        landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")
        landlord_phone = None
        if not landlord_name:
            landlord_name = "Immobilien Jockenhöfer u. Babiel GmbH"
        for item in response.css("#uebersicht span"):
            for line in item.css("*::text").extract():
                if "Tel." in line:
                    landlord_phone = line.replace("Tel.", "").strip()
                    break

        if not 0 <= int(rent) < 40000:
            return

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)

        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("floor", floor)

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("available_date", available_date)
        item_loader.add_value("elevator", elevator)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("images", images)

        self.position += 1
        yield item_loader.load_item()
