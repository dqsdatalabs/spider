# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
import json
from time import time
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, extract_number_only, format_date, extract_location_from_address


class Alberteisele_immobilien_PySpider_germany_de(scrapy.Spider):
    name = "alberteisele_immobilien"
    start_urls = ['https://alberteisele-immobilien.de/#/list1']
    allowed_domains = ["alberteisele-immobilien.de", "immowelt.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    thousand_separator = "."
    scale_separator = ","
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        self.landlord_name = response.css("title::text").get("")
        self.landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").get("").replace("mailto:", "")
        self.landlord_phone = None
        for txt in response.css("#custom_html-15 div::text").extract():
            if "Telefon" in txt:
                self.landlord_phone = txt.split(":").pop().strip()

        match = "var guid = '"
        left = response.text.find(match) + len(match)
        right = left + response.text[left:].find("'")
        guid = response.text[left:right]
        timestamp = int(time()*1000)
        page = response.meta.get("page", 1)
        parsed = urlparse("https://homepagemodul.immowelt.de/list/api/list/")
        url_parts = list(parsed)
        query = dict(parse_qsl(url_parts[4]))
        query.update({
            'callback': "listcallback", 'eType': -1, 'eCat': -1, 'stype': 0,'guid': guid, 'page': page, '_': timestamp,
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        yield scrapy.Request(
            url, callback=self.extract_items,meta={ 'guid': guid, 'page': page },
            headers={ 'Referer': "https://" + self.allowed_domains[0] },
        )

    def extract_items(self, response):
        left = response.text.find("(") + 1
        right = -1
        sel = scrapy.selector.Selector(text=json.loads(response.text[left:right]))
        for box in sel.css(".hm_listbox"):
            title = box.css("a::attr('title')").get()
            if 'reserviert' in title.lower() or "verkaufen" in title.lower() or "büro" in title.lower():
                continue
            item_href = box.css("a::attr('href')").get()
            match = ".ToExpose("
            left = item_href.find(match) + len(match) + 1
            right = left + item_href[left:].find(')') - 1
            item_id = item_href[left:right]
            timestamp = int(time()*1000)
            parsed = urlparse("https://homepagemodul.immowelt.de/home/api/Expose/")
            url_parts = list(parsed)
            query = dict(parse_qsl(url_parts[4]))
            query.update({
                'callback': "exposecallback", 'isStatistic': "false", 'guid': response.meta.get("guid"), 'id': item_id, '_': timestamp,
            })
            url_parts[4] = urlencode(query)
            url = urlunparse(url_parts)
            yield scrapy.Request(
                url,  headers={ 'Referer': "https://" + self.allowed_domains[0] }, callback=self.populate_item,
                meta = { 'title': title, 'external_link': "https://" + self.allowed_domains[0] + "/?page_id=56#/expose" + item_id },
            )
        next_page = sel.xpath("//a[@title='vor']/@class").get() == "hm_btn"
        if next_page:
            yield response.follow(self.start_urls[0], callback=self.parse, meta={ 'page': response.meta.get("page") + 1 }, dont_filter=True)

    def populate_item(self, response):
        left = response.text.find("(") + 1
        right = -1
        sel = scrapy.selector.Selector(text=json.loads(response.text[left:right]))

        rent = currency = utilities = heating_cost = deposit = square_meters = room_count = None
        external_id = floor = parking = available_date = None
        for row in sel.css("ul.hm_objectdata li"):
            key_val = [line.strip() for line in row.css("::text").extract() if line.strip()]
            if len(key_val) != 2:
                continue
            key, val = key_val
            if "Kaltmiete" in key or "Nettomiete" in key:
                rent, currency = extract_rent_currency(val, self.country, Alberteisele_immobilien_PySpider_germany_de)
            elif "Nebenkosten" in key:
                utilities, _ = extract_rent_currency(val, self.country, Alberteisele_immobilien_PySpider_germany_de)
            elif "Heizkosten" in key:
                heating_cost, _ = extract_rent_currency(val, self.country, Alberteisele_immobilien_PySpider_germany_de)
            elif "Kaution" in key:
                deposit, _ = extract_rent_currency(val, self.country, Alberteisele_immobilien_PySpider_germany_de)
            elif "Wohnfläche" in key or "Fläche" in key:
                square_meters = round(float(extract_number_only(val)))
            elif "Zimmer" in key:
                room_count = int(float(extract_number_only(val)))
            elif "Ref." in key:
                external_id = val
            elif "Geschoss" in key:
                floor = val
            elif "Stellplätze" in key or "Stellplatz" in key:
                parking = True
            elif "Bezug" in key:
                available_date = format_date(val, "%d.%m.%Y") if "lieferbar" not in val else None

        bathroom_count = balcony = terrace = elevator = washing_machine = None
        for row in sel.css(".hm_features li *::text").extract():
            row = row.lower()
            if "bad" in row or "gäste-wc" in row:
                bathroom_count = 1
            if "balkon" in row:
                balcony = True
            if "terrasse" in row:
                terrace = True
            if "aufzug" in row:
                elevator = True
            if "wasch" in row:
                washing_machine = True
            if "garage" in row:
                parking = True

        property_type = "apartment" if "wohnung" in response.meta.get("title", "").lower() else "house"

        address_items = sel.css(".hm_expose_half_width > span::text").extract()
        zipcode, city = address_items[1].split(" ")
        address = " ".join(address_items)
        longitude, latitude = extract_location_from_address(address)

        images = sel.xpath("//div[@class='hm_image']//input[contains(@id, 'img')]/@value").extract()
        description = "\r\n".join(sel.css("div.hm_expose_full_width > strong+p::text").extract())

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_link", response.meta.get("external_link"))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", response.meta.get("title"))

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
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", currency)
        item_loader.add_value("heating_cost", heating_cost)

        item_loader.add_value("landlord_name", self.landlord_name)
        item_loader.add_value("landlord_phone", self.landlord_phone)
        item_loader.add_value("landlord_email", self.landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
