import scrapy
from scrapy import Request, FormRequest
from scrapy.http import headers
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs


class ImmobiliaresanguedolceSpider(scrapy.Spider):

    name = 'immobiliaresanguedolce'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['www.immobiliaresanguedolce.it']

    start_urls = ['https://immobiliaresanguedolce.it/it/affitti/?cat=-2%3a1-%7c-2%3a2-%7c-2%3a17-%7c-2%3a15-%7c-2%3a18-%7c-2%3a16-%7c-2%3a25-%7c-2%3a19-%7c-2%3a26-%7c-2%3a20-&ord=prezzo']

    position = 1

    post_url = "https://immobiliaresanguedolce.it/ajax.html?azi=Archivio&lin=it&n="

    formdata = {
        "H_Url": "https://immobiliaresanguedolce.it/it/affitti/?cat=-2:1-|-2:2-|-2:17-|-2:15-|-2:18-|-2:16-|-2:25-|-2:19-|-2:26-|-2:20-&ord=prezzo",
        "Src_Li_Tip": "A",
        "Src_Li_Cat": "-2:1-|-2:2-|-2:17-|-2:15-|-2:18-|-2:16-|-2:25-|-2:19-|-2:26-|-2:20-",
        "Src_Li_Cit": "",
        "Src_Li_Zon": "",
        "Src_T_Pr1": "",
        "Src_T_Pr2": "",
        "Src_T_Mq1": "",
        "Src_T_Mq2": "",
        "Src_T_Cod": "",
        "Src_Li_Ord":"prezzo",
    }


    def parse(self, response):
        yield FormRequest(self.post_url,
                    formdata=self.formdata,
                    callback=self.parse2,
                    dont_filter=True)


    def parse2(self, response):
        
        cards = response.css(".annuncio")

        for index, card in enumerate(cards):

            position = self.position

            property_type = "apartment"

            card_url = card.css("div h2 a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)

            dataUsage = {
                "position": position,
                "property_type": property_type,
                "card_url": card_url,
            }

            ImmobiliaresanguedolceSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        nextPageUrl = response.css(".pagination li.active + li a::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True)



    def parseApartment(self, response):

        external_id = response.css(".caratteristiche .car .lab:contains('Codice') + .val::text").get()
        if external_id:
            external_id = external_id

        square_meters = response.css(".page-section.section-scheda .car-principali .car:nth-child(2)::text").get()
        if square_meters:
            square_meters = remove_white_spaces(square_meters)
            square_meters = extract_number_only(square_meters)

        room_count = response.css(".page-section.section-scheda .car-principali .car:contains('Camere')::text").get()
        if room_count:
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count)
        else:
            room_count = 1

        bathroom_count = response.css(".page-section.section-scheda .car-principali .car:contains('Bagni')::text").get()
        if bathroom_count:
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count)
        else:
            bathroom_count = 1
            
        rent = response.css(".page-section.section-scheda .car-principali .car:contains('€')::text").get()
        if rent:
            rent = extract_number_only(rent).replace(".", "")

        currency = response.css(".page-section.section-scheda .car-principali .car:contains('€')::text").get()
        if currency:
            currency = remove_white_spaces(currency)
            currency = currency_parser(currency, self.external_source)
        else:
            currency = "EUR"

        title = response.css(".page-section.section-scheda h1::text").get()
        if title:
            title = remove_white_spaces(title)

        address = response.css(".indirizzo::text").get()
        if address:
            address = remove_white_spaces(address)
        else:
            address = "not found"

        city = response.css(".caratteristiche .car .lab:contains('Città') + .val::text").get()
        if city:
            city = city

        script_map = response.css("script::text").getall()
        script_map = " ".join(script_map)
        if script_map:
            pattern = re.compile(r'\'map-canvas\', (\d*\.?\d*), (\d*\.?\d*),')
            x = pattern.search(script_map)
            latitude = x.groups()[0]
            longitude = x.groups()[1]

        description = response.css('.description ::text').getall()
        description = " ".join(description)
        description = remove_white_spaces(description)

        images = response.css('.carousel-inner .item img::attr(src)').getall()
        images = [response.urljoin(img) for img in images]
        external_images_count = len(images)

        elevator = response.css(".caratteristiche .car .lab:contains('Accessori') + .val::text").get()
        if "Ascensore" in elevator:
            elevator = True
        else:
            elevator = False


        terrace = response.css(".caratteristiche .car .lab:contains('Terrazzo') + .val::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False

        utilities = response.css(".caratteristiche .car .lab:contains('Spese') + .val::text").get()
        if utilities:
            utilities = remove_white_spaces(utilities)
            utilities = extract_number_only(utilities)

        furnished = response.css(".caratteristiche .car .lab:contains('Arredi') + .val::text").get()
        if furnished:
            furnished = True
        else:
            furnished = False

        landlord_email = "info@immobiliaresanguedolce.it"
        landlord_phone = "0802050106"
        landlord_name = "Immobiliare Sanguedolce & Co s.r.l.s."

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
