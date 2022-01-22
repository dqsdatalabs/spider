# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_location_from_address


def get_property_type(txt):
    if not txt:
        return
    txt = txt.lower()
    if "büro" in txt or "geschäftes" in txt or "schaufen" in txt or "gewerebeein" in txt:
        return
    if "freifläche" in txt or "abstellen" in txt:
        return
    if "haus" in txt:
        return "house"
    if "wohnung" in txt:
        return "apartment"
    return "apartment"

class Theisinger_PySpider_germany_de(scrapy.Spider):
    name = "theisinger"
    start_urls = ['https://www.theisinger-immobilien.de/immo-mieten.xhtml']
    allowed_domains = ["theisinger-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    thousand_separator = "."
    scale_separator = ","
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".list-object"):
            if get_property_type(listing.css("h2::text").get()) is None:
                continue
            url = response.urljoin(listing.css(".image a::attr('href')").get())
            yield scrapy.Request(url, callback=self.populate_item)
        next_page = response.css(".jumpbox-frame span .selected+a::attr('href')").get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def populate_item(self, response):
        title = response.css("h1::text").get()
        property_type = get_property_type(title)
        if property_type is None:
            return

        square_meters = rent = total_rent = deposit = utilities = room_count = bathroom_count = None
        external_id = city = zipcode = country = terrace = balcony = parking = floor = washing_machine = furnished = None
        for row in response.css(".details-desktop tr"):
            key_vals = row.css("td *::text").extract()
            for i in range(0, len(key_vals)-1, 2):
                key, val = key_vals[i:i+2]
                if "ImmoNr" in key:
                    external_id = val
                elif "Ort" in key:
                    city = val
                elif "PLZ" in key:
                    zipcode = val
                elif "Land" in key:
                    country = val
                elif "Wohnfläche" in key:
                    square_meters = int(extract_number_only(val))
                elif "Kaltmiete" in key:
                    rent = int(extract_number_only(val))
                elif "Warmmiete" in key:
                    total_rent = int(extract_number_only(val))
                elif "Kaution" in key:
                    deposit = int(extract_number_only(val))
                elif "Nebenkosten" in key:
                    utilities = int(extract_number_only(val))
                elif "Zimmer" in key:
                    room_count = int(val)
                elif "Badezimmer" in key:
                    bathroom_count = int(val)
                elif "Terrasse" in key:
                    terrace = True
                elif "Balkon" in key:
                    balcony = True
                elif "Stellplätze" in key:
                    parking = True
                elif "Objekttyp" in key:
                    floor = val
                elif "Objektart" in key:
                    if "Wohnung" in val:
                        property_type = "apartment"
                    elif "Haus" in val:
                        property_type = "house"

        address = f"{city} {zipcode}, {country}"
        longitude, latitude = extract_location_from_address(address)
        if not utilities:
            utilities = total_rent - rent

        landlord_name = response.css(".contact p > strong::text").get()
        landlord_phone = response.css(".contact span > span::text").get()
        landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")

        images = response.css("div::attr('data-img')").extract()
        description = response.css(".information *::text").extract()
        description_items = []
        for item in description:
            if "Sonstiges" in item:
                break
            if item.strip():
                if "Balkon" in item:
                    balcony = True
                if "Waschmaschine" in item:
                    washing_machine = True
                if "Stellplatz" in item:
                    parking = True
                if "eingerichtet" in item:
                    furnished = True
                description_items.append(item.strip())
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
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
