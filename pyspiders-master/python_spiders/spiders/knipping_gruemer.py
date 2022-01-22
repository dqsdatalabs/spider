# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_date, extract_location_from_address, get_amenities, description_cleaner


class Knipping_gruemer_PySpider_germany_de(scrapy.Spider):
    name = "knipping_gruemer"
    start_urls = [
        "https://www.knipping-gruemer.de/cms/immobilien/wohnen/mieten/wohnung",
        "https://www.knipping-gruemer.de/cms/immobilien/wohnen/mieten/haus"
    ]
    allowed_domains = ["knipping-gruemer.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        property_type = "house" if "haus" in response.url else "apartment"
        for listing in response.css(".immo-object-wrapper"):
            address = [x.strip() for x in listing.css(".ym-gbox *::text").extract() if x.strip()][0]
            url = listing.css("a::attr('href')").get()
            yield response.follow(url, callback=self.populate_item, meta={ 'property_type': property_type, 'address': address })

        next_page = response.css(".immobox strong+a::attr('href')").get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def populate_item(self, response):
        property_type = response.meta.get("property_type")
        address = response.meta.get("address")
        longitude, latitude = extract_location_from_address(address)
        zipcode = address.split()[0]
        city = " ".join(address.split(" ")[1:])
        external_id = response.css(".expose th::text").get().split(":")[-1].strip()
        title = response.css("#cms-content > h2::text").get()
        if "VERMIETET" in title.replace(" ", "").upper():
            return

        rent = deposit = utilities = square_meters = room_count = bathroom_count = None
        available_date = energy_label = floor = elevator = parking = washing_machine = balcony = terrace = None
        for row in response.css("table.expose tr"):
            key_val = row.css("td::text").extract()
            if len(key_val) != 2:
                continue
            key, val = key_val
            if "Miete" in key and "pro" not in key:
                rent = round(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Badezimmer" in key:
                bathroom_count = round(float(extract_number_only(val)))
            elif "Bad" in key or "Gäste-WC" in key:
                if not bathroom_count:
                    bathroom_count = 1
            elif "verfügbar" in key:
                available_date = extract_date(val)
            elif "Energieeffizienzklasse" in key:
                energy_label = val
            elif "Etage" in key and "Etagen" not in key:
                floor = val
            elif "Fahrstuhl" in key:
                elevator = True
            elif "Garage" in key or "Stellplatz" in key:
                parking = True
            elif "Wasch" in key:
                washing_machine = True
            elif "Balkon" in key or "Terrasse" in key:
                if "Balkon" in key:
                    balcony = True
                if "Terrasse" in key:
                    terrace = True

        landlord_name = response.css(".content-left .no-mobile > p::text").get().strip()
        landlord_phone = landlord_email = None
        for row in response.css(".content-left .no-mobile > p *::text").extract():
            if "@" in row:
                landlord_email = row
            elif "Telefon" in row:
                landlord_phone = row.replace("Telefon:", "").strip()

        images = response.css(".gallerySlideshow-image::attr('src')").extract()
        description_items = response.xpath("//h4[contains(text(), 'Objektbeschreibung')]/following-sibling::p/text()").extract()
        description = description_cleaner("\r\n".join([x.strip() for x in description_items if x.strip()]))

        if "bad" in description.lower() or "gäste-wc" in description.lower():
            if not bathroom_count:
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
        get_amenities(description, "", item_loader)

        self.position += 1
        yield item_loader.load_item()
