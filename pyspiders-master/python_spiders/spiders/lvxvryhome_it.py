# -*- coding: utf-8 -*-
from types import coroutine
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class LvxvryhomeItSpider(scrapy.Spider):
    name = 'lvxvryhome_it'
    allowed_domains = ['lvxvryhome.it']
    start_urls = [
        'https://www.lvxvryhome.it/search/?prov=&cont=affitto&tip=250&fasPrezzo=&fasMQ=&order=9&order=9']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#list>div.annuncio"):
            yield Request(appartment.css("#action > p > a").attrib['href'],
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('#title > h1::text').get().strip()
        address = response.css(
            '#via > p::text').get().strip()
        city = address.split("-")[0].strip()

        rent = response.css(
            '#prezzo > p::text').get().strip().split(" ")[1].strip()

        # if "." in rent:
        #     rent_array = rent.split(".")
        #     rent = rent_array[0] + rent_array[1]

        description_array = response.css(
            "#descrizione > p::text").extract()

        description = ""
        for item in description_array:
            if "www" in item or "numeri" in item or item is None:
                pass
            description += item

        images = response.css('img.gallery::attr(src)').extract()

        features_text = response.css(
            "#datiImmobile > ul > li.txt::text").extract()
        features_values = response.css(
            "#datiImmobile > ul > li.value::text").extract()

        floor = None
        space = None
        rooms = None
        bathrooms = None
        external_id = None
        energy = None
        elevator = None
        for i in range(len(features_text)):
            if "Classe Energetica" in features_text[i]:
                energy = features_values[i].strip()
            elif "Riferimento" in features_text[i]:
                external_id = features_values[i].strip()
            elif "MQ Superficie" in features_text[i]:
                space = features_values[i].strip()
            elif "Nr Vani" in features_text[i]:
                rooms = features_values[i].strip()
            elif "Nr Bagni" in features_text[i]:
                bathrooms = features_values[i].strip()
            elif "Piano" in features_text[i]:
                floor = features_values[i].strip()
            elif "Ascensore" in features_text[i]:
                elevator = features_values[i].strip()
                if "no" in elevator.lower():
                    elevator = False
                else:
                    elevator = True

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)

        item_loader.add_value("energy_label", energy)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "08119718056")
        item_loader.add_value("landlord_email", "lvxvryhome@gmail.com")
        item_loader.add_value("landlord_name", "LUXURY HOME")

        yield item_loader.load_item()
