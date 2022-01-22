# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import property_type_lookup, extract_number_only, extract_rent_currency, extract_location_from_address
import requests


class GesimImmobili_PySpider_italy_it(scrapy.Spider):
    name = "gesim_immobili_it"
    allowed_domains = ["gesimimmobili.it"]
    start_urls = ["https://www.gesimimmobili.it/properties-search/?type=appartamenti-e-loft&status=affitto"]
    execution_type = "testing"
    country = "italy"
    locale = "it"
    thousand_separator = '.'
    scale_separator = ','
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css("article.rh_prop_card--listing"):
            if listing.css(".rh_prop_card__status::text").get().strip().lower() != "affitto":
                continue
            url = listing.css(".rh_overlay__contents a::attr('href')").get()
            if url:
                yield scrapy.Request(url, callback=self.populate_item)

        next_page = response.css(".current + a::attr('href')").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def populate_item(self, response):
        title = response.css(".rh_page__title::text").get().strip()
        external_id = response.css(".id::text").get().strip().split(" ").pop()
        room_count = int(response.css(".prop_bedrooms .figure::text").get())
        bathroom_count = int(response.css(".prop_bathrooms .figure::text").get())
        square_meters = int(response.css(".prop_area .figure::text").get())
        rent, currency = extract_rent_currency(response.css(".price::text").get(), self.country, GesimImmobili_PySpider_italy_it)
        address = response.css(".rh_page__property_address::text").get()
        city, zipcode = address.split(", ")[-3:-1]
        energy_label = response.css(".epc-details span::text").get()
        images = [image for image in response.css(".slides li a::attr('href')").extract() if image.endswith("g")]
        longitude, latitude = extract_location_from_address(address)
        description = response.css(".rh_content p::text").get()
        if not description:
            description = response.css(".rh_content span::text").get()

        balcony = dishwasher = washing_machine = parking = furnished = elevator = None
        sanitized_description = description.lower()
        if "balconi" in sanitized_description:
            balcony = True
        if "lavastoviglie" in sanitized_description:
            dishwasher = True
        if "lavatrice" in sanitized_description:
            washing_machine = True
        if "box auto" in sanitized_description:
            parking = True
        if "arredato" in sanitized_description:
            if "non arredato" in sanitized_description or "vuoto" in sanitized_description:
                furnished = False
            else:
                furnished = True
        if "ascensore" in sanitized_description:
            if "no ascensore" in sanitized_description:
                elevator = False
            else:
                elevator = True

        property_type = floor = None
        for attr in response.css(".rh_property__additional li"):
            key, val = attr.css(".title::text").get(), attr.css(".value::text").get()
            if "TIPOLOGIA" in key:
                property_type = property_type_lookup.get(val.capitalize())
            elif "PIANO" in key:
                floor = val

        terrace = None
        for attr in response.css(".rh_property__feature a::text"):
            if "terrazzo" in attr.get().lower():
                terrace = True

        landlord_email = response.css(".contact-email::attr('href')").get()
        landlord_name = self.name.replace("_", " ").replace("_it", "").capitalize()
        if landlord_email.startswith("mailto:"):
            landlord_email = landlord_email.replace("mailto:", "")

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", f"GesimImmobili_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("latitude", str(latitude))

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("floor", floor)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("parking", parking)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("elevator", elevator)

        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", landlord_name)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        self.position += 1

        yield item_loader.load_item()
