# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_date, extract_location_from_address


class Makler38_PySpider_germany_de(scrapy.Spider):
    name = "makler38"
    start_urls = ['https://www.makler38.de/immobilien-in-braunschweig-wolfsburg-gifhorn-wolfenbuettel-vechelde/']
    allowed_domains = ["makler38.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for url in set(response.css(".item > a::attr('href')").extract()):
            if "buero" in url or "flaeche" in url or "kauf" in url:
                continue
            yield scrapy.Request(url, callback=self.populate_item)

        next_page = response.css(".pagination .active + li + li")
        if next_page.css("::attr('class')").get() != "roundright":
            yield scrapy.Request(next_page.css("a::attr('href')").get(), callback=self.parse)

    def populate_item(self, response):
        property_labels = [x.strip() for x in response.css(".wpestate_estate_property_design_intext_details *::text").extract() if x.strip()]
        property_label = " ".join(property_labels).lower()
        if "kauf" in property_label:
            return

        property_type = None
        if "wohn" in property_label:
            property_type = "apartment"
        elif "haus" in property_label:
            property_type = "house"
        else:
            return

        title = response.css("h1.entry-title::text").get()
        address = " ".join(property_labels[2:4]).strip()
        zipcode, city = address.split()
        longitude, latitude = extract_location_from_address(address)

        rent = utilities = deposit = square_meters = room_count = bathroom_count = None
        external_id = available_date = parking = elevator = energy_label = None
        key = val = None
        for row in response.css(".listing_detail"):
            key_val = [item.strip() for item in row.css("*::text").extract() if item.strip()]
            if len(key_val) == 1:
                key = key_val[0]
                val = None
            elif len(key_val) >= 2:
                key, val = key_val[:2]
            else:
                continue

            if "kaltmiete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Fläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Bad" in key:
                bathroom_count = round(float(extract_number_only(val)))
            elif "Immobilien-ID" in key:
                external_id = val
            elif "Energieeffizienzklasse" in key:
                energy_label = val
            elif "Verfügbar" in key:
                if "sofort" not in val:
                    available_date = extract_date(val)
            elif "stellplat" in key.lower():
                parking = True
            elif "aufzug" in key.lower():
                elevator = True

        landlord_name = response.css(".agent-unit-img-wrapper + div>h4>a::text").get("").strip()
        landlord_phone, landlord_email = response.css(".agent_detail *::text").extract()[:2]

        floor_plan_images = response.css("img.lightbox_trigger_floor::attr('src')").extract()
        images = response.css("img.lightbox_trigger::attr('src')").extract()

        description_items = []
        for item in response.css(".wpestate_estate_property_details_section")[3].css("*::text").extract():
            if "Sonstiges" in item:
                break
            if item.strip():
                description_items.append(item.strip())
        description = "\r\n".join(description_items)

        furnished = dishwasher = washing_machine = balcony = None
        lowered_desc = description.lower()
        if "möblierte" in lowered_desc:
            furnished = True
        if "spüle" in lowered_desc:
            dishwasher = True
        if "wasch" in lowered_desc:
            washing_machine = True
        if "balkon" in lowered_desc:
            balcony = True
        if "stellplätze" in lowered_desc:
            parking = True
        if not bathroom_count and "bad" in lowered_desc:
            bathroom_count = 1
        if "möbliert" in title:
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

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("available_date", available_date)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)

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

        self.position += 1
        yield item_loader.load_item()
