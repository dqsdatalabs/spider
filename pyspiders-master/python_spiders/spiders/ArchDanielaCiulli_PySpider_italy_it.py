# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, extract_location_from_coordinates
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import requests


class ArchDanielaCiulli_PySpider_italy_it(scrapy.Spider):
    name = "arch_daniela_ciulli_it"
    allowed_domains = ["archdanielaciulli.cloud"]
    start_urls = ["http://www.archdanielaciulli.cloud/web/immobili.asp?tipo_contratto=A"]
    execution_type = "testing"
    country = "italy"
    locale = "it"
    thousand_separator = '.'
    scale_separator = ','
    position = 1
    page = 1

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        for url in response.css(".clipimg a::attr('href')").extract():
            base_url = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(response.request.url))
            yield scrapy.Request(base_url + url, callback=self.populate_item, headers=self.headers)

        if response.css("[name=pagina] [selected=selected]+option::text").get():
            self.page += 1
            url_parts = list(urlparse(response.url))
            query = dict(parse_qsl(url_parts[4]))
            query.update({ "num_page": self.page })
            url_parts[4] = urlencode(query)
            yield response.follow(urlunparse(url_parts), callback=self.parse, headers=self.headers)

    def populate_item(self, response):
        title = response.css("h1::text").get()

        lowered_title = title.lower()
        if "commerciale" in lowered_title:
            return

        if "colonica" in lowered_title or "villa" in lowered_title:
            property_type = "house"
        else:
            property_type = "apartment"

        rent, currency = extract_rent_currency(response.css(".prezzodettaglio::text").get(), self.country, ArchDanielaCiulli_PySpider_italy_it)
        area = response.css("#det_prov::attr('data-valore')").get()
        zone = response.css("#det_zona::attr('data-valore')").get()
        square_meters = int(response.css("#det_superficie::attr('data-valore')").get())
        room_count = response.css("#det_camere::attr('data-valore')").get()
        bathroom_count = response.css("#det_bagni::attr('data-valore')").get()
        energy_label = response.css("#det_cl_en::attr('data-valore')").get()
        floor = response.css("#det_piano::attr('data-valore')").get()

        washing_machine = None
        if response.css("#agg1901 strong::text").get() == "Lavanderia":
            washing_machine = True


        latitude = response.css("#map-annuncio::attr('data-lat')").get()
        longitude = response.css("#map-annuncio::attr('data-lng')").get()
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.xpath("//img[@title='Foto']/@src").extract()
        if len(images) == 0:
            images = response.xpath("//img[@title='Esterno']/@src").extract()

        description = "\r\n".join([x.strip() for x in response.css("#block-views-destinations-block::text").extract() if x.strip()])

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", f"ArchDanielaCiulli_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("external_id", response.css("#det_rif::attr('data-valore')").get())
        item_loader.add_value("title", title)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("floor", floor)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("landlord_name", response.xpath("//strong[@itemprop='name']/text()").get())
        item_loader.add_value("landlord_phone", response.xpath("//span[@itemprop='telephone']/text()").get())

        sanitized_description = description.lower()
        if "balconi" in sanitized_description:
            item_loader.add_value("balcony", True)
        if "lavastoviglie" in sanitized_description:
            item_loader.add_value("dishwasher", True)
        if "lavatrice" in sanitized_description:
            item_loader.add_value("washing_machine", True)
        if "box auto" in sanitized_description or "posto macchina" in sanitized_description or "garage" in sanitized_description:
            item_loader.add_value("parking", True)
        if "piscina" in sanitized_description:
            item_loader.add_value("swimming_pool", True)

        if "arredato" in sanitized_description or "arredata" in sanitized_description:
            if "non arredato" in sanitized_description or "vuoto" in sanitized_description or "non arredata" in sanitized_description:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        if "ascensore" in sanitized_description:
            if "no ascensore" in sanitized_description or "senza ascensore" in sanitized_description:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        self.position += 1

        yield item_loader.load_item()
