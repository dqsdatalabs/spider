# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, format_date, extract_location_from_address


class Aktiv_immobilien_PySpider_germany_de(scrapy.Spider):
    name = "aktiv_immobilien"
    start_urls = [
        'https://www.aktiv-immobilien-service.de/ff/immobilien?schema=flat_rent&price=&ffpage=1&sort=date',
        'https://www.aktiv-immobilien-service.de/ff/immobilien?schema=houses_rent&price=&ffpage=1&sort=date',
    ]
    allowed_domains = ["aktiv-immobilien-service.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    landlord_phone = None
    emails = []

    def start_requests(self):
        yield scrapy.Request("https://www.aktiv-immobilien-service.de/kontakt/", callback=self.get_contacts)
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def get_contacts(self, response):
        self.emails = list(map(lambda x: x.replace("mailto:", ""), response.xpath("//a[contains(@href, 'mailto')]/@href").extract()))
        self.landlord_phone = response.xpath("//div[contains(text(), 'Telefon')]/following-sibling::div/text()").get()

    def parse(self, response):
        for listing in response.css(".FFestateview-default-overview-estate"):
            yield scrapy.Request(listing.css("a::attr('href')").get(), callback=self.populate_item)

        next_page = response.css(".ff-next ::attr('data-page')").get()
        if next_page is not None:
            url = response.url
            left = url.find("page=")
            right = left + url[left:].find("&")
            url = url.replace(url[left:right], "page=" + next_page)
            yield scrapy.Request(url, callback=self.parse)

    def populate_item(self, response):
        title = response.css(".content-inner h1::text").get()

        property_type = external_id = available_date = parking = floor = address = zipcode = city = None
        rent = utilities = room_count = square_meters = None
        for row in response.css(".FFestateview-default-details-content-details li"):
            key_val = [x.strip() for x in row.css("span::text").extract() if x.strip()]
            if len(key_val) < 2:
                continue
            key = key_val[0]
            val = " ".join(key_val[1:])

            if "Art" in key:
                if "haus" in val:
                    property_type = "house"
                else:
                    property_type = "apartment"
            elif "Miete" in key:
                rent = int(val.split(",")[0].replace(".", ""))
            elif "Nebenkosten" in key:
                utilities = int(val.split(",")[0].replace(".", ""))
            elif "Zimmer" in key:
                room_count = int(float(val))
            elif "Lage" in key:
                address = val
                for i, item in enumerate(address.split(" ")):
                    if item.isdigit() and len(item) >= 5:
                        zipcode = item
                        break
                city = " ".join(address.split(" ")[i+1:])
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Etagen" in key:
                floor = val
            elif "ID" in key:
                external_id = val
            elif "Stellplatz" in key:
                parking = True
            elif "Bezugstermin" in key:
                if "Vereinbarung" in val:
                    continue
                if "/" in val:
                    val = ".".join((list(map(lambda x: x.replace("/", ""), val.split(".")[-3:]))))
                available_date = format_date(val, "%d.%m.%Y")

        longitude, latitude = extract_location_from_address(address)
        energy_label = response.css("::attr('data-class')").get()
        landlord_name = response.css(".FFestateview-default-details-agent-name span::text").get()
        landlord_phone = response.xpath("//a[contains(@href, 'tel:')]/@href").get("").replace("tel:", "")

        landlord_email = None
        last_name = landlord_phone.split(" ")[-1].lower()
        for email in self.emails:
            if last_name in email:
                landlord_email = email
        if not landlord_email:
            landlord_email = self.emails[0]
        if not landlord_phone:
            landlord_phone = self.landlord_phone

        floor_plan_images = response.css(".FFestateview-default-groundplot img::attr('src')").extract()
        images = response.css(".FFestateview-default-details-main-image img::attr('data-lazy')").extract()
        description_items = [x.strip() for x in response.css(".FFestateview-default-details-content-description *::text").extract() if x.strip()]
        description = []
        for item in description_items:
            if "Sonstiges" in item:
                break
            description.append(item)
        description = "\r\n".join(description)

        washing_machine = balcony = terrace = bathroom_count = swimming_pool = elevator = None
        if "Waschküche" in description or "Wäscherutsche" in description or "Wasch" in description:
            washing_machine = True
        if "Balkon" in description:
            balcony = True
        if "Terrasse" in description:
            terrace = True
        if "Bad" in description or "Duschbad" in description:
            bathroom_count = 1
        if "aufzug" in description:
            elevator = True
        if "Pool" in description:
            swimming_pool = True
        if "Parkplätze" in description:
            parking = True
        
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
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
