# # -*- coding: utf-8 -*-
# # Author: Ahmed Atef
# import scrapy
# from scrapy import Request, FormRequest
# from scrapy.utils.response import open_in_browser
# from scrapy.loader import ItemLoader
# from scrapy.selector import Selector
# from scrapy.utils.url import add_http_if_no_scheme
#
# from ..loaders import ListingLoader
# from ..helper import *
# from ..user_agents import random_user_agent
#
# import requests
# import re
# import time
# from urllib.parse import urlparse, urlunparse, parse_qs
# import json
# from w3lib.html import remove_tags
#
#
# class azimutRealestateSpider(scrapy.Spider):
#
#     country = 'croatia'
#     locale = 'hr'
#     execution_type = 'testing'
#     name = f'azimutRealestate_{locale}'
#     external_source = f"azimutRealestate_PySpider_{country}_{locale}"
#     allowed_domains = ['azimut-realestate.com']
#
#     position = 1
#
#     headers={
#              "Content-Type": "text/html; charset=UTF-8",
#              "Accept": "*/*",
#              "Accept-Encoding": "gzip, deflate, br",
#             }
#
#     rate = currencyExchangeRates("eur", "hrk")
#
#
#
#
#     def start_requests(self):
#         start_urls = [
#             {
#                 'url': 'https://azimut-realestate.com/hr/list?offer_type=rent&category=apartment',
#                 'property_type': 'apartment',
#             },
#             {
#                 'url': 'https://azimut-realestate.com/hr/list?offer_type=rent&category=house',
#                 'property_type': 'house',
#             },
#         ]
#
#         for url in start_urls:
#             yield Request(url=url.get('url'), callback=self.parse, dont_filter=True, meta=url)
#
#
#
#
#     def parse(self, response):
#
#
#         cards = response.css(".iro-content .iro-list-realestate")
#
#         for index, card in enumerate(cards):
#
#             position = self.position
#
#             card_url = card.css("a::attr(href)").get()
#
#             dataUsage = {
#                 "position": position,
#                 "card_url": card_url,
#                 **response.meta
#             }
#
#             azimutRealestateSpider.position += 1
#             yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)
#
#         nextPageUrl = response.css(".iro-content nav li a:contains('>')::attr(href)").get()
#
#
#         if nextPageUrl and nextPageUrl != response.url:
#             yield Request(url = nextPageUrl, callback = self.parse, dont_filter = True, meta=response.meta)
#
#
#
#     def parseApartment(self, response):
#         rent = response.css(".iro-realestate-properties li:contains('Prodajna cijena') span::text").get()
#         if "mj" not in rent:
#             return
#         if "/" in rent:
#             rent = rent.split("/")[1]
#         if rent:
#             rent = remove_white_spaces(rent).replace(" ", "")
#             rent = extract_number_only(rent).replace(".", "")
#             rent = float(rent) * self.rate
#             rent = str(rent).split(".")[0]
#         else:
#             rent = None
#             return
#
#         currency = "HRK"
#
#         property_type = response.meta['property_type']
#
#         position = response.meta['position']
#
#         external_id = response.css(".iro-realestate-properties li:contains('Šifra objekta') span::text").get()
#         if external_id:
#             external_id = remove_white_spaces(external_id)
#             external_id = extract_number_only(external_id)
#
#
#
#         square_meters = response.css(".iro-realestate-properties li:contains('Kvadratura') span::text").getall()
#         if square_meters:
#             square_meters = " ".join(square_meters)
#             square_meters = remove_white_spaces(square_meters).split(",")[0]
#             square_meters = extract_number_only(square_meters).replace(".", "")
#
#         room_count = response.css(".iro-realestate-properties li:contains('Broj soba') span::text").getall()
#         if room_count:
#             room_count = " ".join(room_count)
#             if "studio" in room_count:
#                 room_count = 1
#             else:
#                 room_count = remove_white_spaces(room_count)
#                 room_count = extract_number_only(room_count).replace(".", "")
#
#         bathroom_count = response.css(".iro-realestate-properties li:contains('Broj kupaonica') span::text").getall()
#         if bathroom_count:
#             bathroom_count = " ".join(bathroom_count)
#             bathroom_count = remove_white_spaces(bathroom_count)
#             bathroom_count = extract_number_only(bathroom_count).replace(".", "")
#
#
#
#         description = response.css(".iro-realestate-description p ::text").getall()
#         if description:
#             description = " ".join(description)
#             description = remove_white_spaces(description)
#         else:
#             description = None
#
#         title = response.css(".iro-content-title h1::text").getall()
#         if title:
#             title = " ".join(title)
#             title = remove_white_spaces(title)
#
#         address = response.css(".iro-realestate-properties li:contains('Lokacija') span::text").getall()
#         if address:
#             address = " ".join(address)
#             address = remove_white_spaces(address)
#
#         city = None
#         if address:
#             city = address.split()[0].replace(",","")
#
#         zipcode = None
#         if address:
#             zipcode = extract_number_only(address)
#
#
#
#
#         images = response.css('.iro-realestate-gallery #links a::attr(href)').getall()
#         external_images_count = len(images)
#
#         energy_label = response.css(".iro-realestate-properties li:contains('Energetski razred') span::text").getall()
#         if energy_label:
#             energy_label = " ".join(energy_label)
#             energy_label = remove_white_spaces(energy_label)
#
#         floor = response.css(".iro-realestate-properties li:contains('Kat') span::text").getall()
#         if floor:
#             floor = " ".join(floor)
#             floor = remove_white_spaces(floor)
#
#         parking = response.css(".iro-realestate-properties li:contains('Br. parkirnih mjesta') span::text").get() or response.css(".iro-realestate-features h3:contains('Parking')::text").get()
#         if parking:
#             parking = True
#         else:
#             parking = False
#
#         furnished = response.css(".iro-realestate-features li:contains('Namješteno')::text").get()
#         if furnished:
#             furnished = True
#         else:
#             furnished = False
#
#         elevator = response.css(".iro-realestate-features li:contains('Lift')::text").get()
#         if elevator:
#             elevator = True
#         else:
#             elevator = False
#
#         balcony = response.css(".iro-realestate-features li:contains('Balkon')::text").get()
#         if balcony:
#             balcony = True
#         else:
#             balcony = False
#
#         terrace = response.css(".iro-realestate-features li:contains('Terasa')::text").get()
#         if terrace:
#             terrace = True
#         else:
#             terrace = False
#
#         swimming_pool = response.css(".iro-realestate-features li:contains('Bazen')::text").get()
#         if swimming_pool:
#             swimming_pool = True
#         else:
#             swimming_pool = False
#
#         washing_machine = response.css(".iro-realestate-features li:contains('Perilica rublja')::text").get()
#         if washing_machine:
#             washing_machine = True
#         else:
#             washing_machine = False
#
#         dishwasher = response.css(".iro-realestate-features li:contains('Perilica suđa')::text").get()
#         if dishwasher:
#             dishwasher = True
#         else:
#             dishwasher = False
#
#         pets_allowed = response.css(".iro-realestate-features li:contains('Dozvoljeno kućnim ljubimcima')::text").get()
#         if pets_allowed:
#             pets_allowed = True
#         else:
#             pets_allowed = False
#
#
#
#         landlord_name = response.css(".iro-realestate-agent-info li:nth-child(1)::text").get()
#         if landlord_name:
#             landlord_name = remove_white_spaces(landlord_name)
#
#         landlord_phone = response.css(".iro-realestate-agent-info li:nth-child(2) a::text").get()
#         if landlord_phone:
#             landlord_phone = remove_white_spaces(landlord_phone).replace(" ","")
#
#         landlord_email = response.css(".iro-realestate-agent-info li:nth-child(3)::text").get()
#         if landlord_email:
#             landlord_email = remove_white_spaces(landlord_email)
#
#         if rent:
#             item_loader = ListingLoader(response=response)
#
#             item_loader.add_value("external_link", response.url)
#             item_loader.add_value("external_source", self.external_source)
#             item_loader.add_value("external_id", external_id)
#             item_loader.add_value("title", title)
#             item_loader.add_value("description", description)
#             item_loader.add_value("city", city)
#             item_loader.add_value("zipcode", zipcode)
#             item_loader.add_value("address", address)
#             item_loader.add_value("property_type", property_type)
#             item_loader.add_value("square_meters", square_meters)
#             item_loader.add_value("room_count", room_count)
#             item_loader.add_value("bathroom_count", bathroom_count)
#             item_loader.add_value("images", images)
#             item_loader.add_value("external_images_count", external_images_count)
#             item_loader.add_value("rent", rent)
#             item_loader.add_value("currency", currency)
#             item_loader.add_value("energy_label", energy_label)
#             item_loader.add_value("pets_allowed", pets_allowed)
#             item_loader.add_value("furnished", furnished)
#             item_loader.add_value("floor", floor)
#             item_loader.add_value("parking", parking)
#             item_loader.add_value("elevator", elevator)
#             item_loader.add_value("balcony", balcony)
#             item_loader.add_value("terrace", terrace)
#             item_loader.add_value("swimming_pool",swimming_pool)
#             item_loader.add_value("washing_machine", washing_machine)
#             item_loader.add_value("dishwasher", dishwasher)
#             item_loader.add_value("landlord_name", landlord_name)
#             item_loader.add_value("landlord_email", landlord_email)
#             item_loader.add_value("landlord_phone", landlord_phone)
#             item_loader.add_value("position", position)
#
#             yield item_loader.load_item()
