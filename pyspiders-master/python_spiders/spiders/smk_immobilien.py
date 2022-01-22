# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_location_from_coordinates, extract_date


class Smk_immobilien_PySpider_germany_de(scrapy.Spider):
    name = "smk_immobilien"
    start_urls = ['https://www.smk.immobilien/immobilienangebote/']
    allowed_domains = ["smk.immobilien"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    landlord_email = None
    landlord_phone = None

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        self.landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")
        self.landlord_phone = response.xpath("//a[contains(@href, 'tel')]/@href").get("").replace("tel:", "")
        match = "wlacUnits"
        left = response.text.find(match)
        if left == -1:
            return
        start = left + response.text[left:].find("[")
        end = start + response.text[start:].find(";")
        data = json.loads(response.text[start:end])
        for listing in data:
            if listing.get("Nutzungsart") != "wohnen":
                continue
            if listing.get("Vermarktungsart") != "miete":
                continue
            if "1" in [listing.get(attr) for attr in ["Vermietet", "Reserviert", "Verkauft"]]:
                continue
            latitude = listing.get("Breitengrad")
            longitude = listing.get("Längengrad")
            url = listing.get("Permalink")
            yield scrapy.Request(url, callback=self.populate_item, dont_filter=True, meta={ 'latitude': latitude, 'longitude':longitude })

    def populate_item(self, response):
        title = response.css(".block-title h1::text").get()
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        external_id = property_type = available_date = floor = energy_label = None
        total_rent = rent = utilities = deposit = square_meters = room_count = bathroom_count = None
        for row in response.css(".wlac-field-group"):
            key = row.css(".wlac-field-group-label::text").get()
            val = row.css(".wlac-field-group-value::text").get()
            if "Objektnummer" in key:
                external_id = val.strip()
            elif "Objektart" in key:
                property_type = "apartment" if "Wohnung" in val else None
            elif "Verfügbar" in key:
                available_date = extract_date(val.strip().replace("Vorauss.", ""))
            elif "Zimmer" in key:
                room_count = int(val)
            elif "Bade" in key:
                bathroom_count = int(val)
            elif "Etage" in key:
                floor = val
            elif "Wohnfläche" in key:
                square_meters = int(extract_number_only(val))
            elif "Kaltmiete" in key:
                rent = int(extract_number_only(val))
            elif "Nebenkosten" in key:
                utilities = int(extract_number_only(val))
            elif "Kaution" in key:
                deposit = int(extract_number_only(val))
            elif "Warmmiete" in key:
                total_rent = int(extract_number_only(val))
            elif "Energieeffizienzklasse" in key:
                energy_label = val

        if not rent:
            rent = total_rent
        if not 0 <= int(rent) < 40000:
            return

        landlord_name = " ".join(response.css(".block-broker-text-name::text").get("").strip().split())
        images = response.css(".block-gallery-grid-item img::attr('wlac-image-lazy')").extract()
        description_items = []
        for item in response.css(".block-description *::text").extract():
            if "Sonstige" in item:
                break
            if item.strip():
                description_items.append(item.strip())
        description = "\r\n".join(description_items)

        furnished = terrace = parking = elevator = washing_machine = None
        if "Terrasse" in description:
            terrace = True
        if "möbliert" in description:
            furnished = True
        if "Stellplatz" in description or "stellplatz":
            parking = True
        if "Aufzug" in description:
            elevator = True
        if "Wasch" in description:
            if "Waschmaschine unterzustellen ist hier nicht gegeben" in description:
                washing_machine = False
            else:
                washing_machine = True

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
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
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", self.landlord_phone)
        item_loader.add_value("landlord_email", self.landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
