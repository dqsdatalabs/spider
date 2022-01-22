# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates, format_date


class Bietigheimerwohnbau_PySpider_germany_de(scrapy.Spider):
    name = "bietigheimerwohnbau"
    start_urls = ['https://www.bietigheimer-wohnbau.de/index.php?id=100&type=99900&tx_uoimmo_uoimmo[uoimmo_100][realEstateType]=apartmentRent']
    allowed_domains = ["bietigheimer-wohnbau.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        listings = json.loads(response.body)
        for listing in listings:
            url = f"https://www.bietigheimer-wohnbau.de/angebote/mieten/?tx_uoimmo_uoimmo%5BapartmentRent%5D={listing.get('uid')}&tx_uoimmo_uoimmo%5Baction%5D=show&tx_uoimmo_uoimmo%5Bcontroller%5D=RealEstates"
            yield scrapy.Request(url, callback=self.populate_item, meta={ **listing })

    def populate_item(self, response):
        data = response.meta
        title = data.get("title")
        external_id = data.get("objectid")
        latitude = data.get("latitude")
        longitude = data.get("longitude")
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        floor = str(data.get("numberOfFloors")) + "/" + str(data.get("floor"))
        property_type = "apartment" if "wohnung" in title.lower() else None
        room_count = data.get("numberOfBedRooms")
        bathroom_count = data.get("numberOfBathRooms")
        square_meters = round(data.get("livingSpace"))
        rent = round(data.get("baseRent"))
        utilities = round(data.get("serviceCharge")) + round(data.get("parkingSpacePrice"))
        deposit = int(float(data.get("deposit")))
        currency = "EUR"

        balcony = available_date = energy_label = elevator = None
        for row in response.css(".table_row > .row"):
            key, val = [x.strip().replace("\xad", "") for x in row.css("div::text").extract() if x.strip()]
            if key == "Frei ab":
                available_date = format_date(val, "%d.%m.%Y")
            elif key == "Zimmerzahl":
                if not room_count: room_count = int(val)
            elif key == "Balkon":
                balcony = val == "Ja"
            elif key == "Energieeffizienzklasse":
                energy_label = val
            elif key == "Aufzug":
                elevator = val == "Ja"
        parking = data.get("numberOfParkingSpaces") > 0
        pets_allowed = data.get("petsAllowed") > 0

        landlord_name = data["contactPerson"].get("name")
        landlord_email = data["contactPerson"].get("email")
        landlord_phone = data["contactPerson"].get("phone")

        base_url = "https://" + self.allowed_domains[0] + "/"
        floor_plan_images = [base_url + img['file'] for img in data["groundplans"]]
        images = [base_url + img['file'] for img in data["images"]]

        notes = []
        for line in data.get("otherNote").split("\n"):
            if "http" not in line:
                notes.append(line)
        otherNote = "\n".join(notes)
        description_items = [data.get("descriptionNote"), otherNote]
        description = "/r/n".join(description_items)

        washing_machine = None
        if "Waschmaschin" in description:
            washing_machine = True

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
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("images", images)

        self.position += 1
        yield item_loader.load_item()
