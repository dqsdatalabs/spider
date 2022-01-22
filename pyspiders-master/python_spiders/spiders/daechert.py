# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import format_date, extract_location_from_address


class Daechert_PySpider_germany_de(scrapy.Spider):
    name = "daechert"
    start_urls = ['https://www.daechert-immobilien.de/Mietangebote.htm']
    allowed_domains = ["daechert-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".objekt"):
            if listing.css(".reserviert").get():
                continue
            url = response.urljoin(listing.css("a::attr('href')").get())
            yield scrapy.Request(url, callback=self.populate_item, dont_filter=True)

        next_page = response.css("link[rel='next']::attr('href')").get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse, dont_filter=True)

    def populate_item(self, response):
        title = response.css("h1::text").get()
        property_type = None
        if "wohnung" in response.xpath("//div/i[@class='fa fa-home']/following-sibling::text()").get("").lower():
            property_type = "apartment"
        address = response.xpath("//div/i[@class='fa fa-map-marker']/following-sibling::text()").get()
        zipcode, city = address.split(" ")[:2]
        longitude, latitude = extract_location_from_address(address)
    
        external_id = square_meters = room_count = rent = None
        for row in response.css(".pd > .row > div"):
            key = row.css(".key *::text").get()
            val = row.css(".wert *::text").get()
            if not key or not val:
                continue
            if "Objekt-Nr" in key:
                external_id = val
            elif "Wohnfläche" in key:
                square_meters = round(float(val.split(" ")[0].replace(",", ".")))
            elif "Zimmer" in key:
                room_count = int(val)
            elif "Kaltmiete" in key:
                rent = int(val.split(",")[0].replace(".", ""))

        available_date = utilities = bathroom_count = floor = elevator = parking = washing_machine = balcony = furnished = terrace = None
        for row in response.css(".weiteredaten .row"):
            key = row.css(".key *::text").get()
            val = row.css(".wert *::text").get()
            if "bezugsfrei" in key:
                if "sofort" not in val and "Vereinbarung" not in val and "frei" not in val:
                    available_date = format_date(val.replace("ab", "").strip(), "%d.%m.%Y")
            elif "Nebenkosten" in key:
                utilities = int(val.split(",")[0].replace(".", ""))
            elif "Bad" in key:
                bathroom_count = int(val)
            elif "Etage" in key and "Etagen" not in key:
                floor = val
            elif "Aufzug" in key:
                elevator = True
            elif "Stellplätze" in key or "Stellplatz" in key or "Freiplatz" in key or "garage" in key.lower():
                parking = True
            elif "Wasch" in key:
                washing_machine = True
            elif "Ausstattung" in key:
                furnished = True
            elif "Balkon" in key:
                balcony = True
                if "Terrasse" in key:
                    terrace = True
            elif "Terrasse" in key:
                terrace = True

        landlord_name = response.css(".kontaktname::text").get()
        landlord_phone = response.xpath("//div[@class='kontaktname']/following-sibling::text()").get().replace("Telefon:", "").strip()
        landlord_email =  response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")

        images = []
        for path in response.css(".sliderdiv img::attr('src')").extract():
            images.append(response.urljoin(path))
        description = "\r\n".join(response.css(".beschreibung p::text").extract())

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

        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
