# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_number_only, extract_date, extract_location_from_address, description_cleaner, get_amenities


class Gasteiger_immobilien_PySpider_germany_de(scrapy.Spider):
    name = "gasteiger_immobilien"
    start_urls = ['https://542298.flowfact-sites.net/immoframe/']
    allowed_domains = ["gasteiger-immobilien.de", "flowfact-sites.net"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            for type_filter in ["E4DE337C-2DE8-4560-9D5F-1C33A96037B6|0", "1AB70647-4B47-41E2-9571-CA1CA16E0308|0"]:
                parsed = urlparse(url)
                url_parts = list(parsed)
                query = dict(parse_qsl(url_parts[4]))
                query.update({ 'country': "Deutschland", 'typefilter': type_filter, })
                url_parts[4] = urlencode(query)
                yield scrapy.Request(urlunparse(url_parts), callback=self.parse)

    def parse(self, response):
        for url in response.css(".grid3 > h3 > a::attr('href')").extract():
            yield scrapy.Request(url, callback=self.populate_item)

    def populate_item(self, response):
        external_id = title = property_type = available_date = parking = balcony = terrace = None
        room_count = bathroom_count = square_meters = rent = deposit = utilities = None
        address = zipcode = city = longitude = latitude = None
        for row in response.css(".detaillist tr"):
            key_val = [item.strip() for item in row.css("td *::text").extract() if item.strip()]
            if len(key_val) == 1:
                title = key_val[0]
                continue
            elif len(key_val) != 2:
                continue

            key, val = key_val
            if "Kennung" in key:
                external_id = val
            elif "Objektart" in key:
                if "wohnung" in val:
                    property_type = "apartment"
                elif "haus" in val or "Häuser" in val:
                    property_type = "house"
            elif "Lage" in key:
                address = val
                zipcode = address.split(",")[-1].strip().split()[0]
                city = " ".join(address.split(",")[-1].strip().split()[1:])
                longitude, latitude = extract_location_from_address(address)
            elif "Verfügbar" in key:
                available_date = extract_date(val)
            elif "Zimmer" in key:
                room_count = round(float(extract_number_only(val)))
            elif "Badezimmer" in key:
                bathroom_count = round(float(extract_number_only(val)))
            elif "Bad" in key or "Gäste-WC" in key:
                if not bathroom_count:
                    bathroom_count = 1
            elif "Wohnfläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Miete" in key:
                rent = round(float(extract_number_only(val)))
            elif "Kaution" in key:
                deposit = round(float(extract_number_only(val)))
            elif "Nebenkosten" in key:
                utilities = round(float(extract_number_only(val)))
            elif "Garage" in key or "Stellplatz" in key:
                parking = True
            elif "Balkon" in key or "Terrasse" in key:
                if "Balkon" in key:
                    balcony = True
                if "Terrasse" in key:
                    terrace = True

        landlord_name = response.css(".contact_data strong::text").get()
        landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")
        landlord_phone = None
        fetch_next = False
        for row in response.css(".contact_data *::text").extract():
            if fetch_next:
                landlord_phone = row.strip()
                break
            if "Tel" in row:
                output = row.replace("Telefon:", "").replace("Tel.:", "").strip()
                if not output:
                    fetch_next = True
                else:
                    landlord_phone = output

        images = list(map(response.urljoin, response.css(".slider_thumb > a::attr('rel')").extract()))

        desc_labels = ["Objektbeschreibung", "Ausstattung", "Lage", "Sonstiges", "Energieangaben"]
        desc_items = []
        for label in desc_labels:
            items = response.xpath("//article//h1[contains(text(), '"+label+"')]/../following-sibling::div/text()").extract()
            desc_items.extend([item.strip() for item in items if item.strip()])
        description = description_cleaner("\r\n".join(desc_items))

        if "bad" in description.lower() and not bathroom_count:
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
        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
  
        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        get_amenities(description, "", item_loader)

        self.position += 1
        yield item_loader.load_item()
