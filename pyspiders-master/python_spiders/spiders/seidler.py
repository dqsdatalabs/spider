# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_number_only, extract_date


class Seidler_PySpider_germany_de(scrapy.Spider):
    name = "seidler"
    start_urls = ['https://seidler-immobilien.de/immobilien/']
    allowed_domains = ["seidler-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        data = {
            'post_type': "immomakler_object", 'vermarktungsart': "miete", 'nutzungsart': "wohnen", 'radius': "25", 'von-qm': "0.00",
            'bis-qm': "1160.00", 'von-zimmer': "0.00", 'bis-zimmer': "16.00", 'von-kaltmiete': "0.00", 'bis-kaltmiete': "6800.00",
        }
        for url in self.start_urls:
            parsed = urlparse(url)
            url_parts = list(parsed)
            query = dict(parse_qsl(url_parts[4]))
            query.update(data)
            url_parts[4] = urlencode(query)
            yield scrapy.Request(urlunparse(url_parts), callback=self.parse)

    def parse(self, response):
        for listing in response.css(".property-container"):
            url = listing.css(".thumbnail::attr('href')").get()
            yield scrapy.Request(url, callback=self.populate_item)

    def populate_item(self, response):
        title = response.css("h1::text").get()
        sub_title = [item.strip() for item in response.css(".property-subtitle *::text").extract() if item.strip()]
        address = sub_title[0].split(",")[0].strip()
        property_type = "apartment" if "wohnung" in sub_title[0] else None
        zipcode = address.split(" ")[0]
        city = " ".join(address.split(" ")[1:])
        longitude, latitude = extract_location_from_address(address)

        external_id = floor = available_date = parking = balcony = terrace = elevator = washing_machine = furnished = None
        square_meters = room_count = bathroom_count = rent = utilities = None
        for row in response.css(".property-details .list-group-item"):
            key_val = [item.strip() for item in row.css("div::text").extract() if item.strip()]
            if len(key_val) != 2:
                continue
            key, val = key_val
            if "ID" in key:
                external_id = val
            elif key == "Etage":
                floor = val
            elif "Wohnfläche" in key:
                square_meters = int(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = int(float(extract_number_only(val)))
            elif "Bad" in key:
                bathroom_count = int(float(extract_number_only(val)))
            elif "Kaltmiete" in key:
                rent = int(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = int(float(extract_number_only(val)))
            elif "Verfügbar" in key:
                available_date = extract_date(val.split("/")[-1])
            elif "Stellplätze" in key:
                parking = True
            elif "Balkon" in key:
                balcony = True

        amenities = " ".join(response.css(".property-features li::text").extract()).lower()
        if "stellplatz" in amenities or "garage" in amenities:
            parking = True
        if "balkon" in amenities:
            balcony = True
        if "terrasse" in amenities:
            terrace = True
        if "aufzug" in amenities:
            elevator = True

        landlord_name = response.css(".property-contact .p-name::text").get()
        landlord_email = response.css(".property-contact .email a::text").get()
        landlord_phone = response.css(".property-contact .tel a::text").get()

        images = response.css("#immomakler-galleria a::attr('href')").extract()
        description_items = []
        for item in response.css(".property-description .panel-body *::text").extract():
            if "Sonstige" in item:
                break
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
        if "möbliert" in lowered_desc:
            furnished = True

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
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)
  
        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
