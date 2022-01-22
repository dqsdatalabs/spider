# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, format_date


class Immobilien_meiningen_PySpider_germany_de(scrapy.Spider):
    name = "immobilien_meiningen"
    start_urls = ['http://www.immobilien-meiningen.de/mieten.html']
    allowed_domains = ["immobilien-meiningen.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    company = "Gebhardt & Thelen GmbH & Co. Immobilien KG"

    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
    }

    def get_num(self, string):
       return round(float(string.replace(".", "").replace(",", ".")))

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.headers)

    def parse(self, response, **kwargs):
        match = "sourceIndexUri"
        text = response.text
        left = text.find(match)
        right = left + text[left:].find("\n")
        line = text[left:right]
        left = line.find('"') + 1
        if left == 0:
            left = line.find("'") + 1
            right = left + line[left:].find("'")
        else:
            right = left + line[left:].find('"')
        url = line[left:right]
        yield scrapy.Request(url, callback=self.get_data, headers=self.headers)

    def get_data(self, response):
        json_data = json.loads(response.body)
        for data in json_data["data"]:
            title = data.get("uriident")
            if not title or "buero" in title or "gastronomie" in title or "postamt" in title or "laden" in title or "einzelhaendler" in title:
                continue
            path = "/expose-mieten/expose/" + title + ".html"
            url = "http://" + self.allowed_domains[0] + path

            address_attrs = ["geo-hausnummer", "geo-ort", "geo-strasse", "geo-regionaler_zusatz", "geo-land-@iso_land", "geo-plz"]
            city, zipcode = data.get(address_attrs[3]), data.get(address_attrs[5])
            address = " ".join([data[x] for x in address_attrs if data.get(x)])

            meta_data = { 'address': address, 'title': title, 'city': city, 'zipcode': zipcode }
            yield scrapy.Request(url, callback=self.populate_item, headers=self.headers, meta=meta_data)

    def populate_item(self, response):
        area = response.css(".flaechen-wohnflaeche .value-number::text").get()
        if not area:
            return

        title = response.meta.get("title")
        address = response.meta.get("address")
        city = response.meta.get("city")
        zipcode = response.meta.get("zipcode")
        longitude, latitude = extract_location_from_address(address)

        external_id = response.css(".verwaltung_techn-objektnr_extern .value-text::text").get()
        property_type = "house" if "haus" in title else "apartment"

        rent, currency = response.css(".preise-nettokaltmiete .value-number::text").get().split(" ")
        rent = self.get_num(rent)
        utilities = self.get_num(response.css(".preise-nebenkosten .value-number::text").get().split(" ")[0])
        deposit = self.get_num(response.css(".preise-kaution .value-number::text").get().split(" ")[0])

        square_meters = int(area.split(" ")[0])
        room_count = int(response.css(".flaechen-anzahl_zimmer .value-number::text").get())
        bathroom_count = 0
        if room_count > 1:
            room_count -= 1
            bathroom_count += 1

        available_date = None
        extracted_date = response.css(".verwaltung_objekt-verfuegbar_ab .value-text::text").get().replace("nach Vereinbarung", "")
        extracted_date = extracted_date.replace("ab ", "")
        if extracted_date:
            available_date = format_date(extracted_date, "%d.%m.%Y")

        landlord_name = self.company
        landlord_phone = landlord_email = None
        for footer in response.css(".foot-ul"):
            if footer.css("li::text").get() == "Kontakt":
                landlord_phone = footer.css("li::text").extract()[1][1:].strip()
                landlord_email = footer.css(".email::text").get()

        images = list(map(response.urljoin, response.css(".expose-mini-img img::attr('src')").extract()))
        description = "\r\n".join(response.css(".description::text").extract())

        if not bathroom_count:
            if "Bad" in description:
                bathroom_count = 1
            else:
                bathroom_count = None

        energy_label = None
        match = "Energieeffizienzklasse: "
        index = description.find(match)
        if index > -1:
            index += len(match)
            energy_label = description[index]

        balcony = parking = None
        if "Balkone" in description:
            balcony = True
        if "Stellplatz" in description:
            parking = True
        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", currency)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
