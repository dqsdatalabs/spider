# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, format_date, extract_location_from_address


class Daniela_mauritz_PySpider_germany_de(scrapy.Spider):
    name = "daniela_mauritz"
    start_urls = ['https://daniela-mauritz-immobilien.de/immobilien/']
    allowed_domains = ["daniela-mauritz-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        form_data = {
            'post_type': "immomakler_object", 'post_type': "immomakler_object",
            'vermarktungsart': "miete", 'nutzungsart': "wohnen",
            'columns': 4, 'von-qm': 0.00, 'bis-qm': 145.00,
            'von-zimmer': 0.00, 'bis-zimmer': 5.00,
        }
        for url in self.start_urls:
            parsed = urlparse(url)
            url_parts = list(parsed)
            query = dict(parse_qsl(url_parts[4]))
            query.update(form_data)
            url_parts[4] = urlencode(query)
            yield scrapy.Request(urlunparse(url_parts), callback=self.parse)

    def parse(self, response):
        for listing in response.css(".property-details"):
            url = listing.css("a::attr('href')").get()
            yield scrapy.Request(url, callback=self.populate_item)

        if not response.css(".pages-nav > span:last-of-type > .current").get():
            next_page = None
            for page in response.css(".pages-nav > span"):
                if page.css(".current").get():
                    next_page = page.xpath("following-sibling::span[1]/a/@href").get()
                    break
            yield scrapy.Request(next_page, callback=self.parse)

    def populate_item(self, response):
        title = response.css(".property-title::text").get()
        if "VERMIETET" in title or "RESERVIERT" in title:
            return
        address = response.css(".property-subtitle::text").get("").strip()
        zipcode = address.split(" ")[0]
        city = " ".join(address.split(" ")[1:])
        longitude, latitude = extract_location_from_address(address)

        external_id = landlord_name = landlord_email = landlord_phone = available_date = property_type = None
        rent = utilities = deposit = square_meters = room_count = bathroom_count = None
        balcony = terrace = parking = energy_label = None
        for row in response.css(".list-group-item > .row"):
            key_val = [item.strip() for item in row.css("*::text").extract() if item.strip()]
            key = key_val[0]
            val = "\r\n".join(key_val[1:])

            if "ID" in key:
                external_id = val
            elif "Wohnfläche" in key:
                square_meters = int(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = int(float(val.replace(",", ".")))
            elif "Bade" in key:
                bathroom_count = int(float(val.replace(",", ".")))
            elif "Terrasse" in key:
                terrace = True
            elif "Nebenkosten" in key:
                utilities = int(extract_number_only(val))
            elif "Kaution" in key:
                deposit = int(extract_number_only(val))
            elif "Nettokaltmiete" in key:
                rent = int(extract_number_only(val))
            elif "Stellplatz" in key:
                parking = True
            elif "Balkon" in key:
                balcony = True
            elif "Energie­effizienz­klasse" in key:
                energy_label = val
            elif "Name" in key:
                landlord_name = val
            elif "E-Mail" in key:
                landlord_email = val
            elif "Tel." in key:
                landlord_phone = val
            elif "Verfügbarkeit" in key:
                if "Absprache" in val or "sofort" in val:
                    continue
                available_date = format_date(val, "%d.%m.%Y")
            elif "Objekttypen" in key:
                if "wohnung" in val:
                    property_type = "apartment"
                elif "haus" in val:
                    property_type = "house"

        elevator = washing_machine = None
        for row in response.css(".property-features .list-group-item::text").extract():
            if "aufzug" in row or "Fahrstuhl" in row:
                elevator = True
            elif "Wasch" in row:
                washing_machine = True
            elif "Terasse" in row:
                terrace = True
            elif "Balkon" in row:
                balcony = True
            elif "Garage" in row:
                parking = True

        images = []
        extracted_images = response.css(".attachment-immomakler-gallery-thumb::attr('srcset')").extract()
        for string in extracted_images:
            images.append(string.split(" ")[-2])

        description_items = []
        for row in response.css(".property-description .panel-body *::text"):
            if "Sonstige" in row.get():
                break
            if row.get().strip():
                description_items.append(row.get().strip())
        description = "\r\n".join(description_items)

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
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("available_date", available_date)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
