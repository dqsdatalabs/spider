# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, format_date, extract_rent_currency, extract_location_from_address


class Beudeker_PySpider_germany_de(scrapy.Spider):
    name = "beudeker"
    start_urls = ['https://www.beudeker.de/immobilien-freiburg/immobilie-mieten-freiburg/']
    allowed_domains = ["beudeker.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for listing_url in response.css("a.moreDetails ::attr('href')").extract():
            yield scrapy.Request(response.urljoin(listing_url), callback=self.populate_item)

    def populate_item(self, response):
        title = response.css(".nb-maklerTool-expose-name h1::text").get()
        if "VERMIETET" in title:
            return

        landlord_name = [x for x in response.css(".nb-maklerTool-expose-contact-person .accContent::text").extract() if x.strip()][0]
        landlord_email = response.css(".header-contact-mail::text").get()
        landlord_phone = response.css(".header-contact-phone::text").get()

        external_id = property_type = floor = parking = washing_machine = energy_label = address = None
        room_count = bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        for row in response.css(".stats tr"):
            key = row.css(".label::text").get()
            val = row.css(".value::text").get()
            if key == "Objekt-ID:":
                external_id = val
            elif key == "Immobilienart:":
                property_type = "apartment" if "wohnung" in val else None
            elif key == "Schlafzimmer:":
                room_count = int(val)
            elif key == "Badezimmer:":
                bathroom_count = int(val)
            elif key == "Etage:":
                floor = val
            elif key == "Wohnfläche:":
                square_meters = int(float(extract_number_only(val)))
            elif key == "Verfügbar ab:":
                available_date = format_date(val.split(" ")[0].replace(".20", "."), "%d.%m.%y")
            elif key == "Kaution:":
                deposit = int(extract_number_only(val))
            elif key == "Freiparkplätze:":
                parking = int(val) > 0
            elif key == "Kaltmiete:":
                rent, currency = extract_rent_currency(val, self.country, Beudeker_PySpider_germany_de)
            elif key == "Warmmiete:":
                total_rent, _ = extract_rent_currency(val, self.country, Beudeker_PySpider_germany_de)
            elif key == "Wasch- & Trockenraum:":
                washing_machine = "fa-check" in row.css("i::attr('class')").get()
            elif key == "Energieeffizienzklasse:":
                energy_label = val
            elif key == "Lage:":
                address = val

        utilities = total_rent - rent
        zipcode = address.split(" ")[0]
        city = address.split(" ")[-1]
        longitude, latitude = extract_location_from_address(address)

        raw_description = "\r\n".join([x for x in response.css(".accContent::text").extract() if x.strip()])
        trim_position = raw_description.find("****")
        trimed_description = raw_description[:trim_position]
        description_items = [x for x in trimed_description.split("\r\n") if x.strip()]
        description = "\r\n".join(description_items[:-7])

        balcony = None
        if "Balkon" in description:
            balcony = True

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("title", title)

        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", currency)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("description", description)
        
        self.position += 1
        yield response.follow(
            f"https://www.beudeker.de/index.php?eID=nb_maklertool&action=pic&obj={external_id}&width=940c&height=532c",
            callback=self.get_images,
            meta={ 'item_loader': item_loader },
        )

    def get_images(self, response):
        arr = json.loads(response.body)

        images = []
        for image, label in arr:
            images.append(response.urljoin(image))

        item_loader = response.meta.get('item_loader')
        item_loader.add_value("images", images)

        yield item_loader.load_item()
