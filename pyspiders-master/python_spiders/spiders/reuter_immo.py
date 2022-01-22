# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from  python_spiders.helper import extract_location_from_address


class Reuter_immo_PySpider_germany_de(scrapy.Spider):
    name = "reuter_immo"
    start_urls = ['https://www.reuterimmo.com/alle-immobilien/galerie']
    allowed_domains = ["reuterimmo.com"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for box in response.css(".b01-panel-box"):
            if "Kaufpreis" in box.css("span::text").extract():
                continue
            yield scrapy.Request(response.urljoin(box.css("a::attr('href')").get()), callback=self.populate_item)
        next_page = response.css(".b01-pagination li.b01-active + li>a::attr('href')").get()
        if next_page is not None:
            yield response.follow(response.urljoin(next_page), callback=self.parse)

    def populate_item(self, response):
        rent = utilities = deposit = square_meters = room_count = bathroom_count = total_rooms = parking_cost = None
        pets_allowed = energy_label = balcony = parking = None
        for row in response.css(".b01-table-condensed tr"):
            key_val = row.css("td *::text").extract()
            if len(key_val) == 3:
                key_val.pop()
            elif len(key_val) != 2:
                continue
            key, val = key_val
            if "vermietet" in val.lower() or "vergeben" in val.lower():
                return

            if "Kaltmiete" in key:
                rent = int(val.split(",")[0].replace(".", ""))
            elif "Nebenkosten" in key:
                utilities = int(val.split(",")[0].replace(".", ""))
            elif "Stellplatzmiete" in key:
                parking_cost = int(val.split(",")[0].replace(".", ""))
            elif "Kaution" in key:
                deposit = int(val.split(",")[0].replace(".", ""))
            elif "Wohnfläche" in key:
                square_meters = int(val.split(" ")[0])
            elif "Schlafzimmer" in key:
                room_count = int(val)
            elif "Badezimmer" in key:
                bathroom_count = int(val)
            elif "Haustiere" in key:
                pets_allowed = val != "Nein"
            elif "Energieeffizenzklasse" in key:
                energy_label = val
            elif "Stellplatzart" in key:
                parking = True
            elif "Balkon" in key:
                balcony = True
            elif "Zimmer" in key:
                total_rooms = int(val.split(",")[0])

        utilities = (utilities if utilities else 0) + (parking_cost if parking_cost else 0)
        if not room_count:
            room_count = max(total_rooms-3, 1)
        if not bathroom_count:
            bathroom_count = 1
    
        title = response.css("div::attr('data-title')").get()
        if "VERMIETET" in title or "RESERVIERT" in title:
            return
        property_type = "apartment" if "wohnung" in title.lower() else "house"
        external_id = response.css(".b01-clearfix p strong::text").get()
        address_items = response.css("h3.b01-panel-title + p::text").extract()
        address = ", ".join(address_items)
        zipcode, city = address_items[-1].split(" ")
        longitude, latitude = extract_location_from_address(address)

        landlord_name = response.css("address strong::text").get()
        landlord_phone = response.xpath("//div[text() = 'Telefon Durchwahl']/following-sibling::div[1]/text()").get()

        images = []
        for img in response.css(".b01-grid-small .b01-position-cover::attr('href')"):
            images.append(response.urljoin(img.get()))

        description = None
        for text in response.css(".b01-panel-header p"):
            if text.css("::attr('class')").get():
                continue
            description = "\r\n".join([x.strip() for x in text.css("::text").extract() if x.strip()])
            break

        lowered_desc = description.lower()
        washing_machine = swimming_pool = terrace = None
        if "waschmaschinenanschluss" in lowered_desc:
            washing_machine = True
        if "pool" in lowered_desc:
            swimming_pool = True
        if "terrasse" in lowered_desc:
            terrace = True
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
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

        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
