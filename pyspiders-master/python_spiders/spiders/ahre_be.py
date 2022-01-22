# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import js2xml
import re
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date
from datetime import date
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent


class AhreSpider(scrapy.Spider):

    name = 'ahre_be'
    allowed_domains = ['ahre.be']
    start_urls = ['https://www.ahre.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    thousand_separator = ' '
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = ["https://ahre.be/nos-biens/a-louer/"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@href, "un-bien?d=")]')
        for property_item in listings:
            property_url = property_item.xpath('./@href').extract_first()
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url}
            )

    def get_property_details(self, response):
        external_link = response.meta.get('request_url')
        property_type = response.xpath('.//*[contains(text(), "Référence du bien")]/../text()').extract()[0]
        property_type = property_type.strip().split("-")[0].strip()
        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico", "penthouse", "duplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa','bureau']
        studio_types = ["studio"]
        if property_type.lower() in apartment_types:
            property_type = "apartment"
        elif property_type.lower() in house_types:
            property_type = "house"
        elif property_type.lower() in studio_types:
            property_type = "studio"
        else:
            return
        external_id = external_link.split("d=")[-1]

        address = response.xpath('.//li[contains(text(), "Adresse")]/span/text()').extract_first()
        city = response.xpath('.//*[@class="ville"]/text()').extract_first()
        zipcode = response.xpath('.//*[@class="code_postal"]/text()').extract_first().strip()
        address = ", ".join([address, city, zipcode])

        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_xpath('title', './/*[@class="code_postal"]/../h2/text()')
        item_loader.add_value('address', address)
        item_loader.add_xpath('rent_string', './/*[@class="prix"]/text()')
        item_loader.add_xpath('utilities', './/li[contains(text(), "Charges")]/span/text()')
        item_loader.add_xpath('description', './/*[contains(text(), "Référence du bien")]/../../../div[2]/p/text()')
        item_loader.add_xpath('square_meters', './/li[contains(text(), "Surfaces habitables")]/span/text()')

        item_loader.add_xpath('images', './/a[@class="item"]/@href')
        item_loader.add_xpath('bathroom_count', './/li[contains(text(), "Nombre de salle(s) de bain")]/span/text()')
        item_loader.add_xpath('energy_label', './/li[contains(text(), "PEB valeur")]/span/text()')
        item_loader.add_xpath('room_count', './/li[contains(text(), "Nombre des pieces")]/span/text()')
        if not item_loader.get_output_value('room_count'):
            item_loader.add_xpath('room_count','.//h6[contains(text(), "Chambre(s)")]/following-sibling::p/text()')
        item_loader.add_xpath('floor', './/li[contains(text(), "Etage du bien")]/span/text()')
        item_loader.add_xpath('landlord_name', './/h6[contains(text(), "Conseiller Immobilier")]/../h5/text()')
        item_loader.add_xpath('landlord_email', './/*[contains(text(), "EMAIL")]/..//a[contains(@href, "mailto:")]/text()')
        item_loader.add_xpath('landlord_phone', './/*[contains(text(), "TÉLEPHONE")]/..//a[contains(@href, "tel:")]/text()')
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)

        terrace = response.xpath('.//li[contains(text(), "Terrasse")]/span/text()').extract_first()
        if terrace is None:
            terrace = response.xpath('.//ul[@class="list-inline detail_liste_picto"]//*[contains(text(), "Terrasse")]/text()').extract_first()
        if terrace:
            if terrace == "Non":
                item_loader.add_value('terrace', False)
            else:
                item_loader.add_value('terrace', True)
        elevator = response.xpath('.//li[contains(text(), "Ascenseur")]/span/text()').extract_first()
        if elevator:
            if elevator == "Non":
                item_loader.add_value('elevator', False)
            else:
                item_loader.add_value('elevator', True)
        balcony = response.xpath('.//li[contains(text(), "Balcon")]/span/text()').extract_first()
        if balcony:
            if balcony == "Non":
                item_loader.add_value('balcony', False)
            else:
                item_loader.add_value('balcony', True)

        parking = response.xpath('.//li[contains(text(), "Garage")]/span/text()').extract_first()
        if parking:
            if parking == "Non":
                item_loader.add_value('parking', False)
            else:
                item_loader.add_value('parking', True)

        if 'meubl\u00e9' in item_loader.get_output_value('description').lower() and 'non meubl\u00e9' not in item_loader.get_output_value('description').lower():
            item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Ahre_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
