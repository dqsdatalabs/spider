import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time


class LemurapisaSpider(scrapy.Spider):

    name = 'lemurapisa'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    allowed_domains = ['www.lemurapisa.it']
    start_urls = ['https://www.lemurapisa.it/it/immobili?contratto=2&tipologia%5B%5D=1&tipologia%5B%5D=50&tipologia%5B%5D=42&tipologia%5B%5D=39&provincia=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&rif=&order_by=prezzo&order_dir=asc']

    position = 1
############################################################################################################################

    def parse(self, response):

        cards = response.css(".properties-grid .property")

        for index, card in enumerate(cards):

            position = self.position
            card_url = card.css(".title a::attr(href)").get()
            if card_url:
                card_url = response.urljoin(card_url)

            square_meters = card.css(".area .value::text").get()
            if square_meters:
                square_meters = square_meters

            room_count = card.css(".bedrooms .content::text").get()
            if room_count:
                room_count = convert_string_to_numeric(
                    room_count, LemurapisaSpider)

            bathroom_count = card.css(".bathrooms .content::text").get()
            if bathroom_count:
                bathroom_count = bathroom_count

            rent = card.css(".image .price::text").get()
            if rent:
                rent = convert_string_to_numeric(rent, LemurapisaSpider)

            currency = card.css(".image .price::text").get()
            if currency:
                currency = currency_parser(currency, self.external_source)

            property_type = card.css(".location::text").get()
            if property_type:
                property_type = property_type_lookup[property_type]

            title = card.css(".title h2 a::text").get()
            if title:
                title = title.strip()

            dataUsage = {
                "card_url": card_url,
                "position": position,
                "square_meters": square_meters,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "rent": rent,
                "currency": currency,
                "property_type": property_type,
                "title": title,
            }

            LemurapisaSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        nextPageUrl = response.css(
            ".pagination .pagination li a[rel='next']::attr(href)").get()
        if nextPageUrl:
            nextPageUrl = response.urljoin(nextPageUrl)

        if nextPageUrl:
            yield Request(url=nextPageUrl, callback=self.parse, dont_filter=True)

    def parseApartment(self, response):

        external_id = response.css(
            ".property-detail .pull-left table tr th:contains('Rif.:') + td::text").get()
        if external_id:
            external_id = external_id

        description = response.css('.property-detail p::text').getall()
        if description:
            description = " ".join(description)
            description = remove_white_spaces(description)

        address = response.css(".sidebar .widget-text address::text").getall()
        if address:
            address = " ".join(address)
            address = remove_white_spaces(address)

        city = response.css(".sidebar .widget-text address::text").getall()
        if city:
            city = remove_white_spaces(address)
            city = city.split(" ")[-1]

        images = response.css('#slider ul.slides li img::attr(src)').getall()
        external_images_count = len(images)

        energy_label = response.css(
            ".property-detail .pull-left table tr th:contains('Classe energ.:') + td::text").get()
        if energy_label:
            energy_label = energy_label

        elevator = response.css(
            ".property-detail .pull-left table tr th:contains('Ascensore') + td::text").get()
        if elevator == "sì":
            elevator = True

        furnished = response.css(
            ".property-detail .pull-left table tr th:contains('Arredato') + td::text").get()
        if furnished == "arredato":
            furnished = True
        elif furnished == "parzialmente arredato":
            furnished = True
        elif furnished == "non arredato":
            furnished = False

        balcony = response.css(
            ".property-detail .pull-left table tr th:contains('Balconi') + td::text").get()
        if balcony == "sì":
            balcony = True

        terrace = response.css(
            ".property-detail .pull-left table tr th:contains('Terrazzi') + td::text").get()
        if terrace == "sì":
            terrace = True

        landlord_email = response.css(
            'footer .contact tr th.email + td a::text').get()
        if landlord_email:
            landlord_email = landlord_email.strip()

        landlord_phone = response.css(
            'footer .contact tr th.phone + td::text').get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()

        landlord_name = response.css(
            'footer .properties .content strong::text').get()
        if landlord_name:
            landlord_name = landlord_name.strip()

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", response.meta['title'])
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", response.meta['square_meters'])
        item_loader.add_value("room_count", response.meta['room_count'])
        item_loader.add_value(
            "bathroom_count", response.meta['bathroom_count'])
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
        pass


def get_p_type_string(p_type_string):

    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number >= 92:
        energy_label = "A"
    elif energy_number >= 81 and energy_number <= 91:
        energy_label = "B"
    elif energy_number >= 69 and energy_number <= 80:
        energy_label = "C"
    elif energy_number >= 55 and energy_number <= 68:
        energy_label = "D"
    elif energy_number >= 39 and energy_number <= 54:
        energy_label = "E"
    elif energy_number >= 21 and energy_number <= 38:
        energy_label = "F"
    else:
        energy_label = "G"
    return energy_label
