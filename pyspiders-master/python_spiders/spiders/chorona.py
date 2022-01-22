# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_location_from_address, extract_date, get_amenities, description_cleaner


class Chorona_PySpider_germany_de(scrapy.Spider):
    name = "chorona"
    start_urls = ['https://www.immobilien-at-webcore.de/nutzer/0009/auswerten_immo_neu.php']
    allowed_domains = ["chorona.de", "immobilien-at-webcore.de"]
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
            query.update({ 'Art': 1, 'ISeite': "0001", 'sort': "Datum DESC" })
            url_parts[4] = urlencode(query)
            yield scrapy.Request(urlunparse(url_parts), callback=self.parse)

    def parse(self, response):
        for url in response.css("#erg_objekte > #erg_foto > a::attr('href')").extract():
            yield response.follow(url, callback=self.populate_item)

    def populate_item(self, response):
        title, external_id = response.css("#obj_titel::text").extract()
        external_id = external_id.replace("Objekt-Nr.:", "").strip()

        address_items = [item.strip() for item in response.css("#obj_daten_sp2 > #obj_daten_eintrag > b::text").extract() if item.strip()]
        address = ", ".join(address_items)
        zipcode = address_items[0].split(" ")[0].strip()
        city = " ".join(address_items[0].split(" ")[1:]).strip()
        longitude, latitude = extract_location_from_address(address)

        property_type = available_date = energy_label = floor = None
        room_count = bathroom_count = square_meters = rent = utilities = deposit = None
        for row in response.css("#obj_daten_eintrag"):
            if "Objekttyp" in row.css("#obj_daten_eintrag2_sp1::text").get(""):
                val = row.css("#obj_daten_eintrag2_sp2::text").get("").lower()
                if "wohnung" in val:
                    property_type = "apartment"
                elif "haus" in val:
                    property_type = "house"

            key = row.css("#obj_daten_eintrag_sp1 *::text").get()
            val = row.css("#obj_daten_eintrag_sp2 *::text").get()
            if key is None or val is None:
                continue

            if "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Kaltmiete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Frei" in key:
                available_date = extract_date(val)

        amenities = []
        for item in response.css("#obj_merk_eintrag1::text").extract() + response.css("#obj_merk_eintrag2::text").extract():
            if "Energieeffizienzklasse" in item:
                energy_label = item.replace("Energieeffizienzklasse:", "").strip()
            elif "Etage" in item:
                floor = item.replace("Etage:", "").strip()
            if item.strip():
                amenities.append(item.strip().lower())
        amenities = " ".join(amenities)

        contact_data = [x.strip() for x in response.css("#obj_ap_daten *::text").extract() if x.strip()]
        landlord_name = contact_data[0]
        landlord_phone = contact_data[1]
        landlord_email = contact_data[-3] + "@" + contact_data[-1].replace(" [dot] ", ".").strip()

        floor_plan_images = None
        floor_plan_path = response.css("#obj_pdf_txt a::attr('href')").get()
        if floor_plan_path:
            floor_plan_images = [response.urljoin(floor_plan_path)]

        images = list(map(response.urljoin, response.css(".sp-thumbnail::attr('src')").extract()))
        description = description_cleaner("\r\n".join(response.css("#obj_beschr *::text").extract()))

        if ("bad" in description or "bad" in amenities) and not bathroom_count:
            bathroom_count = 1
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

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        get_amenities(description, amenities, item_loader)

        self.position += 1
        yield item_loader.load_item()
