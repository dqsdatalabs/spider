# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, format_date, extract_location_from_address


class Pistoor_PySpider_germany_de(scrapy.Spider):
    name = "pistoor"
    start_urls = ['https://www.pistoor.de/immobilien/']
    allowed_domains = ["pistoor.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        data = {
            'post_type': "immomakler_object", 'vermarktungsart': "miete", 'nutzungsart': "wohnen", 'radius': "25", 'von-qm': "0.00",
            'bis-qm': "485.00", 'von-zimmer': "0.00", 'bis-zimmer': "10.00", 'von-kaltmiete': "0.00", 'bis-kaltmiete': "1000.00",
        }
        for url in self.start_urls:
            parsed = urlparse(url)
            url_parts = list(parsed)
            query = dict(parse_qsl(url_parts[4]))
            query.update(data)
            url_parts[4] = urlencode(query)
            yield scrapy.Request(urlunparse(url_parts), callback=self.parse)

    def parse(self, response):
        self.landlord_name = response.xpath("//span[contains(@itemprop, 'name')]/text()").get()
        self.landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")
        self.landlord_phone = response.xpath("//a[contains(@href, 'tel')]/@href").get("").replace("tel:", "")
        for url in response.css(".property-container > a::attr('href')").extract():
            yield scrapy.Request(url, callback=self.populate_item)

    def populate_item(self, response):
        title = response.css("h1::text").get()

        external_id = property_type = available_date = parking = balcony = address = city = zipcode = longitude = latitude = floor = None
        room_count = bathroom_count = square_meters = rent = utilities = deposit = None
        for row in response.css(".property-details .row"):
            key_val = [item.strip() for item in row.css("div::text").extract() if item.strip()]
            if len(key_val) == 3 and "Adresse" in key_val:
                address = " ".join(key_val[1:])
                zipcode = key_val[-1].split("\u00a0")[0]
                city = " ".join(key_val[-1].split("\u00a0")[1:])
                longitude, latitude = extract_location_from_address(address)
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
            elif "Nebenkosten" in key:
                utilities = int(extract_number_only(val))
            elif "Kaution" in key:
                deposit = int(extract_number_only(val))
            elif "Kaltmiete" in key:
                rent = int(extract_number_only(val))
            elif "Stellplatz" in key or "Stellplätze" in key:
                parking = True
            elif "Balkon" in key:
                balcony = True
            elif key == "Etage":
                floor = val
            elif "Verfügbar" in key:
                if "Absprache" in val or "sofort" in val:
                    continue
                available_date = format_date(val, "%d.%m.%Y")
            elif "Objekttypen" in key:
                if "wohnung" in val:
                    property_type = "apartment"
                elif "haus" in val:
                    property_type = "house"

        energy_label = elevator = terrace = None
        for row in response.css(".property-epass .row"):
            key_val = [item.strip() for item in row.css("div::text").extract() if item.strip()]
            if len(key_val) != 2:
                continue
            key, val = key_val
            if "Energie­effizienz­klasse" in key:
                energy_label = val

        amenities = " ".join(response.css(".avia_textblock li::text").extract()).lower()
        if "stellplatz" in amenities:
            parking = True
        if "balkon" in amenities:
            balcony = True
        if "aufzug" in amenities:
            elevator = True

        images = response.css(".sp-image::attr('data-large')").extract()
        description_items = []
        for row in response.css(".property-description .panel-body *::text"):
            if "Sonstige" in row.get():
                break
            if row.get().strip():
                description_items.append(row.get().strip())
        description = "\r\n".join(description_items)

        if "Stellplatz" in description:
            parking = True
        if "Balkon" in description:
            balcony = True
        if "terrasse" in description.lower():
            terrace = True
        if "aufzug" in description.lower():
            elevator = True

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
        item_loader.add_value("terrace", terrace)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", self.landlord_name)
        item_loader.add_value("landlord_phone", self.landlord_phone)
        item_loader.add_value("landlord_email", self.landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
