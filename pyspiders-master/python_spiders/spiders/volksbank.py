# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_date, description_cleaner, get_amenities, extract_location_from_address


class Volksbank_PySpider_germany_de(scrapy.Spider):
    name = "volksbank"
    start_urls = [
        "https://www.immo-volksbank.de/mieten/wohnungen",
        "https://www.immo-volksbank.de/mieten/haeuser",
    ]
    allowed_domains = ["immo-volksbank.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".card-estate"):
            url = listing.css("a::attr('href')").get()
            if url is None:
                continue
            address = [x.strip() for x in listing.css(".card-text *::text").extract() if x.strip()][0]
            yield response.follow(url, callback=self.populate_item, meta={ 'address': address })

    def populate_item(self, response):
        address = response.meta.get("address")
        zipcode = address.split(" ")[0]
        longitude, latitude = extract_location_from_address(address)

        title = response.css(".container h1::text").get()
        property_type = None
        if "wohnung" in title.lower():
            property_type = "apartment"
        elif "haus" in description.lower():
            property_type = "house"

        external_id = available_date = energy_label = city = None
        room_count = bathroom_count = square_meters = rent = utilities = None
        for row in response.css(".estate-list li"):
            key, val = row.css("span::text").extract()
            if "Objektnummer" in key:
                external_id = val.replace("VI-Nr.", "").strip()
            elif "Verfügbar" in key:
                available_date = extract_date(val)
            elif "Energieklasse" in key:
                energy_label = val
            elif "Ort" in key:
                city = val
            elif "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Kaltmiete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))

        if not city:
            city = " ".join(address.split(",")[-1].strip().split()[1:])

        landlord_name = response.css(".mt-3.py-3 img::attr('alt')").get()
        landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")
        landlord_phone = None
        for text in [x.strip() for x in response.css(".mt-3.py-3 *::text").extract() if x.strip()]:
            if text.startswith("T"):
                landlord_phone = text.replace("T", "").strip()

        images = response.xpath("//*[contains(@data-fancybox, 'estate-images')]/@href").extract()
        images.extend(response.xpath("//*[contains(@data-fancybox, 'estate-images')]/@data-src").extract())

        description = "\r\n".join([x.strip() for x in response.css(".container > h1 + .row > div:last-child > p::text").extract() if x.strip()])
        description = description_cleaner(description)

        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, "", ListingLoader(response=response))
        if "kein fahrstuhl" in description:
            elevator = False
        if "haustiere sind nicht erlaubt":
            pets_allowed = False
        if "bad" in description:
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

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("available_date", available_date)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)

        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
