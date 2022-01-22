# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse, urlencode, parse_qsl, urlunparse
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_location_from_address, extract_rent_currency, format_date


class Otte_immobilien_PySpider_germany_de(scrapy.Spider):
    name = "otte_immobilien"
    start_urls = [
        "https://www.otte-immobilien.de/mieten-in-coburg/wohnung-mieten-coburg/",
        "https://www.otte-immobilien.de/mieten-in-sonneberg/wohnungen-mieten-in-sonneberg/",
        "https://www.otte-immobilien.de/mieten-in-coburg/haeuser-zu-mieten-in-coburg/",
        "https://www.otte-immobilien.de/mieten-in-sonneberg/haeuser-mieten-in-sonneberg/",
    ]
    allowed_domains = ["otte-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1
    emails = {}

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for office_data in response.css(".widget"):
            office_city = office_data.css(".widgettitle::text").get()
            if not office_city:
                continue
            office_city = office_city.replace("Büro ", "")
            self.emails[office_city] = office_data.css(".textwidget::text").extract().pop().strip()

        for listing in response.css(".openestate_listing_entry"):
            attrs = listing.css(".col_2 li::text").extract()
            area = int(float(extract_number_only(attrs[0])))
            if area > 10:
                url = listing.css(".headlist a::attr('href')").get()
                yield scrapy.Request(url, callback=self.populate_item)

    def populate_item(self, response):
        title = response.css("#openestate_header h1::text").get()

        external_id = property_type = address = city = zipcode = None
        address_parts = []
        for row in response.css("#ex_info li::text").extract():
            key_val = row.split(":")
            if len(key_val) != 2:
                continue
            key, val = map(str.strip, key_val)
            if key == "Objekt-Nr":
                external_id = val
            elif key == "Immobilienart":
                property_type = "apartment" if "wohnung" in val.lower() else "house"
            elif key == "Adresse":
                address_sub_parts = val.split(" ")
                zipcode = address_sub_parts[0]
                city = address_sub_parts[1]
                address_parts.append(val)
            elif key == "Region":
                address_parts.append(val)
        address = ", ".join(address_parts)
        longitude, latitude = extract_location_from_address(address)

        square_meters = room_count = bathroom_count = rent = currency = utilities = deposit = total_rent = None
        elevator = available_date = energy_label = pets_allowed = balcony = terrace = parking = None
        for row in response.css("#openestate_expose_view_content li"):
            key = row.css("::text").get()
            val = row.css("b::text").get()
            if not key:
                continue
            if "Kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, Otte_immobilien_PySpider_germany_de)
            elif "Betriebs-/ Nebenkosten" in key:
                utilities = int(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = int(float(extract_number_only(val)))
            elif "Wohnfläche" in key:
                square_meters = int(float(extract_number_only(val)))
            elif "Zimmerzahl" in key:
                room_count = int(extract_number_only(val))
            elif "Anzahl Badezimmer" in key:
                bathroom_count = int(extract_number_only(val))
            elif "Anzahl Schlafzimmer" in key:
                room_count = int(extract_number_only(val))
            elif "Personenaufzug" in key:
                elevator = val == "ja"
            elif "verfügbar ab" in key and "sofort" not in val:
                available_date = format_date(val, "%d.%m.%Y")
            elif "Energieeffizienzklasse" in key:
                energy_label = val
            elif "Haustiere" in key:
                pets_allowed = val == "ja"
            elif "Stellplatz" in key:
                parking = True
            elif "Warmmiete" in key:
                total_rent, _ = extract_rent_currency(val, self.country, Otte_immobilien_PySpider_germany_de)
            elif "balkon" in key.lower() or "terrace" in key.lower():
                if "balkon" in key.lower():
                    balcony = True
                if "terrasse" in key.lower():
                    terrace = True
        if total_rent:
            utilities = total_rent - rent

        landlord_data = response.css("#openestate_expose_contact_person li::text").extract()
        office_city = response.css("h3.av-special-heading-tag::text").get().split(" ").pop()
        landlord_name = landlord_data[0]
        landlord_phone = landlord_data[2].replace("Telefon:", "")
        landlord_email = self.emails.get(office_city)

        images = []
        for url in response.css("#openestate_expose_gallery_thumbnails img::attr('src')").extract():
            url_parts = list(urlparse(url))
            query = dict(parse_qsl(url_parts[4]))
            if "x" in query:
                query["x"] = int(query["x"]) * 10
            if "y" in query:
                query["y"] = int(query["y"]) * 10
            url_parts[4] = urlencode(query)
            images.append(urlunparse(url_parts))
        description = "\r\n".join(response.css("#openestate_expose_view_content p::text").extract())

        if not bathroom_count:
            if "Badezimmer" in description:
                room_count -= 1
                bathroom_count = 1

        floor = None
        if property_type == "apartment" and ("1. Obergeschoss" in description or "1. OG" in description):
            floor = "1"
        elif property_type == "apartment" and "Erdgeschoss" in description:
            floor = "0"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("title", title)

        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)

        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images) # Array
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
