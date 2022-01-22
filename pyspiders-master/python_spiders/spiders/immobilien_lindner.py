# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces, extract_location_from_address


class Immobilien_lindner_PySpider_germany_de(scrapy.Spider):
    name = "immobilien_lindner"
    start_urls = ['https://www.immobilien-lindner.de/?page_id=48']
    allowed_domains = ["immobilien-lindner.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en',
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        meta = {}
        title = None
        description_items = []
        for line in response.css(".art-postcontent p"):
            key = " ".join(line.css("strong::text").extract()).strip()
            val = " ".join(line.css("::text").extract()).strip()
            if "___" not in val:
                description_items.append(remove_white_spaces(val))
            if "Raum" in key:
                title = key
            elif "age" in key:
                meta['address'] = [x for x in val.split("\xa0") if x.strip()][1].strip()
            elif "Wohnfläche" in key:
                meta['square_meters'] = int(float(extract_number_only(val)))
            elif "Kaution" in key:
                if "Kaltmieten" in key:
                    meta['deposit'] = int(extract_number_only(key)) * meta["rent"]
                else:
                    meta['deposit'] = int(extract_number_only(key))
            elif "Kaution" in val:
                if "Kaltmieten" in val:
                    meta['deposit'] = int(extract_number_only(val)) * meta["rent"]
                else:
                    meta['deposit'] = int(extract_number_only(val))
            elif "Kaltmiete" in key:
                rent_string = key.split("+")
                meta['rent'] = round(float(extract_number_only(rent_string[0])))
                meta['utilities'] = int(extract_number_only(rent_string[1]))
                if "EUR" in rent_string[0] or "EUR" in rent_string[1]:
                    meta['currency'] = "EUR"
                if len(rent_string) > 2:
                    if "Heizkosten" in rent_string[2]:
                        meta['heating_cost'] = int(extract_number_only(rent_string[2]))
                    else:
                        meta['utilities'] += int(extract_number_only(rent_string[2]))
                        if "Stellplatz" in rent_string[2]:
                            meta['parking'] = True

            elif "___" in key or "___" in val:
                if not meta:
                    continue
                item_loader = self.populate_item(response, { **meta, 'title': title, 'description': "\r\n".join(description_items) })
                yield response.follow(
                    "https://www.immobilien-lindner.de/?page_id=19", callback=self.get_contact_details,
                    meta={ 'item_loader': item_loader }, dont_filter=True,
                )
                meta = {}
                description_items = []
            else:
                if "Bad" in val:
                    meta['bathroom_count'] = 1
                if "Stellplatz" in val:
                    meta['parking'] = True
                if "Balkon" in val:
                    meta['balcony'] = True

    def populate_item(self, response, meta):
        title = meta.get("title")
        city = title.split(" ").pop()

        room_count = int(extract_number_only(title))
        bathroom_count = meta.get("bathroom_count")
        square_meters = meta.get("square_meters")

        rent = meta.get("rent")
        currency = meta.get("currency")
        utilities = meta.get("utilities")
        deposit = meta.get("deposit")
        heating_cost = meta.get("heating_cost")

        parking = meta.get("parking")
        balcony = meta.get("balcony")

        address = ", ".join([meta.get("address", ""), city])
        longitude, latitude = extract_location_from_address(address)
        landlord_name = response.css("title::text").get().replace(": Mietwohnungen", "")
        description = meta.get("description")

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", f"{self.start_urls[0]}#{self.position}")
        item_loader.add_value("title", f"{title} #{self.position}")

        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", currency)

        item_loader.add_value("heating_cost", heating_cost)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("description", description)

        self.position += 1
        return item_loader

    def get_contact_details(self, response):
        landlord_email = response.css("td a::text").get()
        landlord_phone = None
        for line in response.css("td p::text"):
            if line.get().startswith("Tel. :"):
                landlord_phone = line.get().replace("Tel. :", "").strip()

        item_loader = response.meta.get("item_loader")
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        yield item_loader.load_item()
