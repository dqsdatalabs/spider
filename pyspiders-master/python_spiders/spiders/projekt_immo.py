# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_date, extract_location_from_address, get_amenities, description_cleaner


class Projekt_immo_PySpider_germany_de(scrapy.Spider):
    name = "projekt_immo"
    start_urls = [
        "https://www.projekt-immo.de/immobilien/wohnen/mieten/wohnungen",
        "https://www.projekt-immo.de/immobilien/wohnen/mieten/haeuser",
    ]
    allowed_domains = ["projekt-immo.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".listing-object-wrapper"):
            property_type = "apartment" if "wohnung" in response.url else "house"
            url = listing.css("a::attr('href')").get()
            yield response.follow(url, callback=self.populate_item, meta={ 'property_type': property_type })
        next_page = response.css(".pagination li.active + li>a::attr('href')").get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def populate_item(self, response):
        title = response.css(".expose > h1::text").get()
        if "VERMIETET" in title.replace(" ", "").upper():
            return

        property_type = response.meta.get("property_type")
        external_id = [x.strip() for x in response.css(".valign-btn::text").get("").replace("Objekt-Nr.", "").split("\n") if x.strip()][0][:-1]

        rent = utilities = heating_cost = deposit = square_meters = room_count = bathroom_count = None
        address = zipcode = city = latitude = longitude = floor = available_date = energy_label = None
        parking = elevator = balcony = terrace = washing_machine = pets_allowed = None
        for row in response.css(".table-condensed td"):
            key_val = [x.strip() for x in row.css("*::text").extract() if x.strip()]
            key = key_val[0]
            val = None
            if len(key_val) > 1:
                val = key_val[1]

            if "Miete" in key:
                if rent:
                    rent = min(rent, round(float(extract_number_only(val))))
                else:
                    rent = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key and "in" not in key:
                utilities = round(float(extract_number_only(val)))
            elif "Heizkosten" in key and "in" not in key:
                heating_cost = round(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Badezimmer" in key:
                bathroom_count = round(float(extract_number_only(val)))
            elif "Bad" in key or "Gäste-WC" in key:
                if not bathroom_count:
                    bathroom_count = 1
            elif "Energieeffizienzklasse" in key:
                energy_label = val
            elif "Etage" in key and "Etagen" not in key:
                floor = val
            elif "Adresse" in key:
                address = val
                zipcode = address.split(",")[-1].strip().split()[0]
                city = " ".join(address.split(",")[-1].strip().split()[1:])
                longitude, latitude = extract_location_from_address(address)
            elif "verfügbar" in key:
                if "sofort" in val.lower() or "Absprache" in val or "Vereinbarung" in val or "n.V." in val:
                    continue
                available_date = extract_date(val)
                if available_date is not None and "/" in available_date:
                    input_string = ".".join(val.split(".")[:2] + ["20" + val.split(".")[-1]])
                    available_date = extract_date(input_string)
            elif "Stellplätze" in key or "Garage" in key:
                parking = True
            elif "Fahrstuhl" in key:
                elevator = True
            elif "Balkon" in key:
                balcony = True
            elif "Terrasse" in key:
                terrace = True
            elif "Haustiere" in key:
                pets_allowed = row.css(".fa-check").get() is not None
            elif "Wasch" in key:
                washing_machine = row.css(".fa-check").get() is not None

        landlord_name = landlord_phone = landlord_email = None
        contact_data = [x.strip() for x in response.css(".panel .panel-body *::text").extract() if x.strip()]
        landlord_name = contact_data[0]
        for data in contact_data:
            if "Telefon:" in data:
                landlord_phone = data.replace("Telefon:", "").strip()
            if "@" in data:
                landlord_email = data.strip()

        images = response.css(".sc-media-gallery::attr('href')").extract()
        description_items = [x.strip() for x in response.xpath("//div[contains(@class, 'clear')]/div/*/text()").extract() if x.strip()]
        description = description_cleaner("\r\n".join(description_items))

        if "bad" in description.lower() and not bathroom_count:
            bathroom_count = 1

        if not 0 <= int(rent) < 40000:
            return
        if not rent:
            rent = None
        if not utilities:
            utilities = None
        if not deposit:
            deposit = None
        if not heating_cost:
            heating_cost = None
        if not room_count:
            room_count = None
        if not bathroom_count:
            bathroom_count = None
        if not square_meters:
            square_meters = None

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
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("heating_cost", heating_cost)
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        get_amenities(description, "", item_loader)

        self.position += 1
        yield item_loader.load_item()
