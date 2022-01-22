# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_location_from_address


class Geno24_PySpider_germany_de(scrapy.Spider):
    name = "geno24"
    start_urls = ['https://geno-24.de/openimmo/search']
    allowed_domains = ["geno-24.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            parsed = urlparse(url)
            url_parts = list(parsed)
            query = dict(parse_qsl(url_parts[4]))
            query.update({ 'o_type': 5726, 'o_option': "miete_pacht", 'o_location': 0, 'region': "DEU", })
            url_parts[4] = urlencode(query)
            url = urlunparse(url_parts)
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".openimmo-search-item"):
            url = listing.css("a::attr('href')").get()
            yield response.follow(url, callback=self.populate_item)

    def populate_item(self, response):
        title = response.css("title::text").get()
        key = val = None
        square_meters = room_count = rent = utilities = None
        external_id = address = zipcode = city = latitude = longitude = property_type = energy_label = None
        for i, item in enumerate(response.css(".openimmo-properties > *")):
            if i % 2 == 0:
                key = item.css("::text").get()
                continue
            val = item.css("::text").get()
            if "ID" in key:
                external_id = val
            elif "Lage" in key:
                address = val
                zipcode, city = address.split()
                longitude, latitude = extract_location_from_address(address)
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val.replace(".", ","))))
            elif "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Kaltmiete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Energieeffizienzklasse" in key:
                energy_label = val
            elif "Objektart" in key:
                if "Wohnung" in val:
                    property_type = "apartment"
                elif "Haus" in val:
                    property_type = "house"

        contact_ref = response.xpath("//h2[contains(text(), 'Kontakt')]/following-sibling::p")
        landlord_name = contact_ref.css("::text").get("").strip()
        landlord_phone = contact_ref.xpath("following-sibling::p/text()").get("").replace("Tel.", "").strip()
        landlord_email = contact_ref.xpath("following-sibling::p/following-sibling::p/*/text()").get("").strip()

        images = []
        for style in response.css(".carousel-item::attr('style')").extract():
            match = "url("
            left = style.find(match) + len(match) + 1
            right = left + style[left:].find(")") - 1
            images.append(response.urljoin(style[left:right]))

        description = response.xpath("//h2[contains(text(), 'Beschreibung')]/following-sibling::p/text()").get("").strip()

        lowered_desc = description.lower()
        bathroom_count = washing_machine = balcony = elevator = parking = None
        if "bad" in lowered_desc:
            bathroom_count = 1
        if "wasch" in lowered_desc:
            washing_machine = True
        if "balkon" in lowered_desc:
            balcony = True
        if "fahrstuhl" in lowered_desc or "aufzug" in lowered_desc:
            elevator = True
        if "stellplatz" in lowered_desc or "garage" in lowered_desc:
            parking = True

        if not 0 <= int(rent) < 40000:
            return

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_link", response.url)
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

        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("energy_label", energy_label)

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
