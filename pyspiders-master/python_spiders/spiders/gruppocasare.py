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


class GruppocasareSpider(scrapy.Spider):

    name = 'gruppocasare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Gruppocasare_PySpider_" + country + "_" + locale

    allowed_domains = ['www.gruppocasare.it']
    start_urls = ['https://www.gruppocasare.it/risultati-ricerca/?keyword=&location=&area=&property_id=&bedrooms=&type=appartamento&status=affitto&min-price=&max-price=&sortby=a_price']

    position = 1

    def parse(self, response):

        cards = response.css(".property-listing .row div.item-wrap")

        for index, card in enumerate(cards):

            position = self.position
            card_url = card.css(".phone a::attr(href)").get()

            square_meters = card.css(
                ".amenities.hide-on-grid p:nth-of-type(1) span:contains('mq')::text").get()
            if square_meters:
                square_meters = square_meters.split(" ")[1]

            room_count = card.css(
                ".amenities.hide-on-grid p:nth-of-type(1) span:contains('Locale')::text,.amenities.hide-on-grid p:nth-of-type(1) span:contains('Locali')::text").get()
            if room_count:
                room_count = convert_string_to_numeric(
                    room_count, GruppocasareSpider)

            bathroom_count = card.css(
                ".amenities.hide-on-grid p:nth-of-type(1) span:contains('Bagno')::text,.amenities.hide-on-grid p:nth-of-type(1) span:contains('Bagni')::text").get()
            if bathroom_count:
                bathroom_count = bathroom_count.split(" ")[1]

            rent = card.css(".price.info-row .item-price::text").get()
            if rent:
                rent = convert_string_to_numeric(rent, GruppocasareSpider)

            currency = card.css(".price.info-row .item-price::text").get()
            if currency:
                currency = currency_parser(currency, self.external_source)

            dataUsage = {
                "card_url": card_url,
                "position": position,
                "square_meters": square_meters,
                "room_count": room_count,
                "bathroom_count": bathroom_count,
                "rent": rent,
                "currency": currency,
            }

            GruppocasareSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

    def parseApartment(self, response):

        property_type = "apartment"

        external_id = response.css(
            ".detail-list ul li:contains('Riferimento')::text").get()
        if external_id:
            external_id = external_id

        zipcode = response.css(".detail-address ul .detail-zip::text").get()
        if zipcode:
            zipcode = zipcode

        address = response.css(
            ".detail-address ul .detail-address::text").get()
        if address:
            address = address

        city = response.css(".detail-address ul .detail-city::text").get()
        if city:
            city = city

        action = "houzez_get_single_property"
        prop_id = response.css("#prop_id::attr(value)").get()
        security = response.css("#securityHouzezMap::attr(value)").get()
        responseSomeData = requests.post(
            "https://www.gruppocasare.it/wp-admin/admin-ajax.php",
            data={
                "action": action,
                "prop_id": prop_id,
                "security": security,
            })
        dataResponseSomeData = responseSomeData.json()["props"][0]

        latitude = dataResponseSomeData["lat"]
        longitude = dataResponseSomeData["lng"]
        title = dataResponseSomeData["title"]
        external_images_count = dataResponseSomeData["images_count"]

        description = response.css(
            '.property-description > p::text, .property-description > p strong::text').getall()
        description = " ".join(description)

        energy_label = response.css(
            '.houzez-energy-container .houzez-energy-table dt:contains("Classe energetica") + dd::text').get()

        images = response.css('.lightbox-slide .item img::attr(src)').getall()
        external_images_count = len(images)

        elevator = response.css(
            ".detail-features ul li a:contains('Ascensore')::text").get()
        if elevator:
            elevator = True

        furnished = response.css(
            ".detail-features ul li a:contains('Arredato')::text").get()
        if furnished:
            furnished = True

        balcony = response.css(
            ".detail-features ul li a:contains('Balcone')::text").get()
        if balcony:
            balcony = True

        terrace = response.css(
            ".detail-features ul li a:contains('Terrazzo')::text").get()
        if terrace:
            terrace = True

        landlord_email = response.css(
            '[name="target_email"]::attr(value)').get()
        if landlord_email:
            landlord_email = landlord_email.strip()
        else:
            landlord_email = response.css(
                '.footer .widget-contact ul li:nth-of-type(3) a::text').get()

        landlord_phone = response.css('.hz-agent-phone span::text').get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
        else:
            landlord_phone = response.css(
                '.footer .widget-contact ul li:nth-of-type(2)::text').get()

        landlord_name = response.css(
            '.media.agent-media dl dd:first-child::text').get()
        if landlord_name:
            landlord_name = landlord_name.strip()
        else:
            landlord_name = "gruppocasare"

        parking = response.css(".detail-features ul li a:contains('Posto auto')::text").get()
        if parking:
            parking = True
        else:
            parking = False
            
            
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "Gruppocasare_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("property_type", property_type)
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
        item_loader.add_value("parking", parking)
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
