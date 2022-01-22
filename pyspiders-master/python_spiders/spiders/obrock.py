# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_location_from_address, extract_date


class Obrock_PySpider_germany_de(scrapy.Spider):
    name = "obrock"
    start_urls = [
        "https://www.obrock.de/angebote/?_search=true&mt=rent&category=14&address=&sort=date%7Cdesc#immobilien",
        "https://www.obrock.de/angebote/?_search=true&mt=rent&category=10&address=&sort=date%7Cdesc#immobilien",
        "https://www.obrock.de/angebote/?_search=true&mt=rent&category=15&address=&sort=date%7Cdesc#immobilien",
    ]
    allowed_domains = ["obrock.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        self.landlord_name = response.css("title::text").get("").split("|").pop().strip()
        self.landlord_phone = response.xpath("//a[contains(@href, 'tel:')]/@href").get("").replace("tel:", "")
        self.landlord_email = response.xpath("//a[contains(@href, 'mailto:')]/@href").get("").replace("mailto:", "")

        for listing in response.css(".immo-listing__wrapper"):
            url = listing.css("a::attr('href')").get()
            yield scrapy.Request(url, callback=self.populate_item)

        next_page = response.css(".pages .next::attr('href')").get()
        if next_page is not None:
            yield scrapy.Request(next_page, callback=self.parse)

    def populate_item(self, response):
        property_type = response.css(".h5 > .badge-secondary::text").get().split()[0].lower()
        if "zimmer" in property_type:
            property_type = "room"
        elif "haus" in property_type:
            property_type = "house"
        elif "wohnung" in property_type:
            property_type = "apartment"

        title = response.css("h1::text").get()
        arranged_title = title.replace(" ", "").upper()
        if "VERMIETET" in arranged_title or "PROVISIONSFREI" in arranged_title:
            return

        address = response.xpath("//span[contains(@class, 'badge-secondary')]/following-sibling::text()").get("").strip()
        zipcode = address.split()[0]
        city = " ".join(address.split()[1:])
        longitude, latitude = extract_location_from_address(address)
        images = response.css(".lightgallery a::attr('href')").extract()

        external_id = floor = rent = utilities = deposit = heating_cost = room_count = bathroom_count = square_meters = None
        description = available_date = energy_label = parking = elevator = balcony = terrace = pets_allowed = washing_machine = None
        for script in response.css("script.vue-tabs::text"):
            sel = scrapy.selector.Selector(text=script.get())
            for row in sel.css(".expose-data li"):
                key = row.css(".key::attr('title')").get()
                if not key:
                    key = row.css(".key::text").get("").strip()
                if not key:
                    continue
                val = row.css(".value::attr('title')").get()
                if not val:
                    val = row.css(".value::text").get("").strip()

                if "Miete" in key or "Kaltmiete" in key:
                    rent = round(float(extract_number_only(val)))
                elif "Nebenkosten" in key:
                    utilities = round(float(extract_number_only(val)))
                elif "Kaution" in key:
                    deposit = round(float(extract_number_only(val)))
                elif "Heizkosten" in key:
                    heating_cost = round(float(extract_number_only(val)))
                elif "Wohnfläche" in key:
                    square_meters = round(float(extract_number_only(val)))
                elif "Zimmer" in key:
                    room_count = round(float(extract_number_only(val)))
                elif "Badezimmer" in key:
                    bathroom_count = round(float(extract_number_only(val)))
                elif "Bad" in key or "Gäste-WC" in key:
                    if not bathroom_count:
                        bathroom_count = 1

                elif "Objekt-Nr" in key:
                    external_id = val
                elif "Etage" in key and "Etagen" not in key:
                    floor = val
                elif "Energieeffizienzklasse" in key:
                    energy_label = val
                elif "verfügbar" in key:
                    if "sofort" in val or "Absprache" in val or "Vereinbarung" in val:
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

            amenities = " ".join(sel.css(".immo-expose__bool-fields li::text").extract()).lower()
            if "fahrstuhl" in amenities:
                elevator = True
            if "wasch" in amenities:
                washing_machine = True
            if "haustiere" in amenities:
                pets_allowed = True
            if "bad" in amenities or "gäste-wc" in amenities:
                if not bathroom_count:
                    bathroom_count = 1

            description_items = [
                "\r\n".join(sel.xpath("//*[contains(@title, 'Beschreibung')]/*/text()").extract()).strip(),
                "\r\n".join(sel.xpath("//*[contains(@title, 'Ausstattung')]/*/text()").extract()).strip(),
                "\r\n".join(sel.xpath("//*[contains(@title, 'Lage')]/*/text()").extract()).strip(),
            ]
            if not [item.strip() for item in description_items if item.strip()]:
                continue
            description = "\r\n".join(description_items)

            lowered_desc = description.lower()
            if "aufzug" in lowered_desc or "fahrstuhl" in lowered_desc:
                elevator = True
            if "wasch" in lowered_desc:
                washing_machine = True
            if "balkon" in lowered_desc:
                balcony = True
            if "terrasse" in lowered_desc:
                terrace = True
            if "stellplatz" in lowered_desc:
                parking = True
            if "bad" in lowered_desc or "gäste-wc" in lowered_desc:
                if not bathroom_count:
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
        item_loader.add_value("energy_label", energy_label)
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

        item_loader.add_value("landlord_name", self.landlord_name)
        item_loader.add_value("landlord_phone", self.landlord_phone)
        item_loader.add_value("landlord_email", self.landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
