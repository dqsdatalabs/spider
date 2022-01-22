# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_number_only, extract_date


class Ewg_dresden_PySpider_germany_de(scrapy.Spider):
    name = "ewg_dresden"
    start_urls = ['https://www.ewg-dresden.de/Wohnungen.html']
    allowed_domains = ["ewg-dresden.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        sel = None
        if "plugins" in response.url:
            sel = scrapy.selector.Selector(text=response.text)
        else:
            sel = response
        for i, listing in enumerate(sel.css(".objekt")):
            if listing.css(".lazyMore"):
                yield scrapy.http.FormRequest(
                    "https://www.ewg-dresden.de/plugins/immobilien/ajax_immobilien_lazyload.php",
                    callback=self.parse,
                    formdata={ 'aktPos': str(i), 'live': "1", }
                )
                return
            path = listing.css(".objektContent a::attr('href')").get()
            if "Wohnung" in path or "Haus":
                yield scrapy.Request(f"https://{self.allowed_domains[0]}/{path}", callback=self.populate_item)

    def populate_item(self, response):
        title = response.css("#wohnung h1::text").get()
        external_id = response.url.split("/")[-1].split("-")[0]
        property_type = "house" if "haus" in response.url.lower() else "apartment"
        address = response.css("#wohnung h2::text").get()
        zipcode = address.split(",")[-1].split()[0].strip()
        city = " ".join(address.split(",")[-1].split()[1:])
        longitude, latitude = extract_location_from_address(address)

        key = val = None
        square_meters = room_count = rent = utilities = deposit = floor = available_date = None
        for i, item in enumerate(response.css(".wEigenschaften > dl > *")):
            if i % 2 == 0:
                key = item.css("::text").get()
                continue
            val = item.css("::text").get()
            if "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Kaltmiete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Genossenschaftsanteil" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Etage" in key:
                floor = val
            elif "Verfügbarkeit" in key:
                if "sofort" in val:
                    continue
                available_date = extract_date(val)

        amenities = " ".join(response.css(".features li::text").extract()).lower()
        bathroom_count = balcony = terrace = elevator = None
        if "bad" in amenities:
            bathroom_count = 1
        if "balkon" in amenities:
            balcony = True
        if "terrasse" in amenities:
            terrace = True
        if "aufzug" in amenities:
            elevator = True

        landlord_name = response.css(".sidebarAP h4::text").get()
        landlord_phone = response.css(".sidebarAP .icon-telefon").xpath("following-sibling::text()").get()
        landlord_email = response.css(".sidebarAP .icon-mail").xpath("following-sibling::*/text()").get()

        floor_plan_images = [response.urljoin(response.css(".lytebox::attr('href')").get())]
        images = list(map(response.urljoin, response.css(".wohnungsThumbs > a::attr('href')").extract()))
        images.extend(response.css(".wohnungsPic img::attr('src')").extract())
        description = response.xpath("//h2[contains(text(), 'beschreibung')]/following-sibling::text()").get()

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
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)

        item_loader.add_value("rent", rent)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
