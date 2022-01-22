# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_date, extract_location_from_address, get_amenities


class Fundament_PySpider_germany_de(scrapy.Spider):
    name = "fundament"
    start_urls = ['https://www.fundament-haus.de/wohnungen/']
    allowed_domains = ["fundament-haus.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".property-listing-simple"):
            if listing.css(".meta-item-value::text").extract().pop() == "Vermietet":
                continue
            url = listing.css("a::attr('href')").get()
            yield scrapy.Request(url, callback=self.populate_item)

    def populate_item(self, response):
        title = response.css(".page-title::text").get()

        square_meters = room_count = bathroom_count = property_type = None
        for row in response.css(".meta-inner-wrapper"):
            key = row.css(".meta-item-label::text").get("").strip()
            val = row.css(".meta-item-value::text").get("").strip()
            if "Bereich" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Schlafzimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Typ" in key:
                if "Wohnung" in val:
                    property_type = "apartment"
                elif "Haus" in val:
                    property_type = "house"
            elif "Status":
                if "Vermietet" in val:
                    return

        amenities = ""
        address = rent = utilities = deposit = available_date = floor = parking = washing_machine = None
        for row in response.css(".property-additional-details-list dl"):
            key = row.css("dt::text").get("").strip()
            val = row.css("dd::text").get("").strip()
            if "Anschrift" in key:
                address = val
                address_item = val.split(", ")[1]
                zipcode = address_item.split(" ")[0].strip()
                city = " ".join(address_item.split(" ")[1:]).strip()
                longitude, latitude = extract_location_from_address(address)
            elif "Kaltmiete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Bezug" in key:
                available_date = extract_date(val)
            elif "Geschoss" in key:
                floor = val
            elif "Stellplatz" in key:
                parking = True
            elif "Ausstattungsmerkmale" in key:
                amenities = val.lower().strip()

        landlord_name = response.css(".agent-name a::text").get()
        landlord_phone = response.xpath("//a[contains(@href, 'tel')]/@href").get("").replace("tel:", "").replace("/", "")
        landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")

        floor_plan_images = response.css(".floor-plan-map a::attr('href')").extract()
        images = response.css(".slides li a::attr('href')").extract()

        if "bad" in amenities:
            bathroom_count = 1
        if "wasch" in amenities:
            washing_machine = True
        if not 0 <= int(rent) < 40000:
            return

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
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
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("images", images)
        get_amenities("", amenities, item_loader)

        self.position += 1
        yield item_loader.load_item()
