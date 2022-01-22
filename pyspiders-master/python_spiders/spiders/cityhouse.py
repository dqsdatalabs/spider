# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_location_from_address, extract_date


class Cityhouse_PySpider_germany_de(scrapy.Spider):
    name = "cityhouse"
    start_urls = ['https://www.cityhouse-immobilien.de/immobilienangebote/immobilienangebote-miete/']
    allowed_domains = ["cityhouse-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        self.landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")
        self.landlord_phone = response.xpath("//a[contains(@href, 'tel')]/@href").get("").replace("tel:", "")
        for listing in response.css(".property-container"):
            property_type = listing.css(".property-subtitle::text").get("")
            if "Büro" in property_type or "Verkaufsfläche" in property_type or "Praxis" in property_type:
                continue
            if listing.css(".property-status-vermietet").get():
                continue
            url = listing.css("a.thumbnail::attr('href')").get()
            yield scrapy.Request(url, callback=self.populate_item)

    def populate_item(self, response):
        title = response.css(".property-title::text").get()
        subtitle = response.css(".property-subtitle::text").get()
        subtitle1, subtitle2 = [item.strip() for item in subtitle.split(",")]
        property_type = "apartment" if "wohnung" in subtitle2 else None
        zipcode = subtitle1.split(" ")[0]
        city = " ".join(subtitle1.split(" ")[1:])

        external_id = address = available_date = parking = balcony = floor = None
        room_count = bathroom_count = square_meters = rent = utilities = deposit = heating_cost = None
        for row in response.css(".property-details .row"):
            key_val = [item.strip() for item in row.css("div::text").extract() if item.strip()]
            if len(key_val) > 2 and "Adresse" in key_val:
                address = " ".join(key_val[1:])
                continue
            elif len(key_val) != 2:
                continue

            key, val = key_val
            if "ID" in key:
                external_id = val
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = int(float(val.replace(",", ".")))
            elif "Bade" in key:
                bathroom_count = int(float(val.replace(",", ".")))
            elif "Betriebskosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Kaltmiete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Heizkosten" in key:
                heating_cost = round(float(extract_number_only(val)))
            elif "Stellplatz" in key or "Stellplätze" in key:
                parking = True
            elif "Balkon" in key:
                balcony = True
            elif "Etage" in key:
                floor = val
            elif "Verfügbar" in key:
                if "Absprache" in val or "sofort" in val.lower() or "Vereinbarung" in val:
                    continue
                extracted_date = extract_date(val.replace("01. ", ""))
                if len(extracted_date.split("-")) == 2:
                    extracted_date += "-01"
                available_date = extracted_date
            elif "Objekttypen" in key:
                if "wohnung" in val:
                    property_type = "apartment"
                elif "haus" in val:
                    property_type = "house"

        longitude, latitude = extract_location_from_address(address)
        energy_label = elevator = washing_machine = None
        for row in response.css(".property-epass .row"):
            key_val = [item.strip() for item in row.css("div::text").extract() if item.strip()]
            if len(key_val) != 2:
                continue
            key, val = key_val
            if "Energie­effizienz­klasse" in key:
                energy_label = val

        amenities = " ".join(response.css(".property-features li::text").extract()).lower()
        if "stellplatz" in amenities or "garage" in amenities:
            parking = True
        if "balkon" in amenities:
            balcony = True
        if "aufzug" in amenities:
            elevator = True

        landlord_name = response.css(".property-contact .p-name::text").get()
        images = response.css("#immomakler-galleria img::attr('data-big')").extract()
        description_items = []
        for item in response.css(".property-description .panel-body *::text").extract():
            if "Sonstige" in item:
                break
            if "Wir" in item or "cityhouse" in item.lower():
                continue
            if item.strip():
                description_items.append(item.strip())
        description = "\r\n".join(description_items)

        lowered_desc = description.lower()
        if "stellplatz" in lowered_desc or "garage" in lowered_desc:
            parking = True
        if "balkon" in lowered_desc:
            balcony = True
        if "aufzug" in lowered_desc:
            elevator = True
        if "wasch" in lowered_desc:
            washing_machine = True

        if not 0 <= int(rent) < 40000:
            return

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

        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("heating_cost", heating_cost)
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", self.landlord_phone)
        item_loader.add_value("landlord_email", self.landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
