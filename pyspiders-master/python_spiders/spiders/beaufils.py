# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import js2xml
import re
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only
from datetime import date
import dateparser

def extract_city_zipcode(_address):
    if ", " in _address:
        zip_city = _address.split(", ")[1]
    else:
        zip_city = _address
    zipcode = zip_city.split(" ")[0]
    city = re.sub(r"\d+", "", zip_city).strip()
    return zipcode, city


class BeaufilsSpider(scrapy.Spider):

    name = 'beaufils_be'
    allowed_domains = ['beaufils.be']
    start_urls = ['https://www.beaufils.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.beaufils.be/fr/a-louer?view=list&page=1&view=list&ptype=1',
                'property_type': 'house'},
            {'url': 'https://www.beaufils.be/fr/a-louer?view=list&page=1&view=list&ptype=2',
             'property_type': 'apartment'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@href, "id=")]')
        for property_item in listings:
            property_url = response.urljoin(property_item.xpath('./@href').extract_first())
            property_url = re.sub(r"&page=\d+&", "&", property_url)
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'property_type': response.meta.get('property_type')}
            )

        next_page_url = response.xpath('.//a[@class="nav next"]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta.get('property_type')}
                )

    def get_property_details(self, response):
        external_link = response.meta.get('request_url')
        property_type = response.meta.get('property_type')
        external_id = external_link.split("id=")[1].split("&")[0]
        available_date = ''.join(response.xpath('.//div[contains(text(), "Disponible")]/../div[@class="value"]/text()').extract())
        parking = ''.join(response.xpath('.//div[contains(text(), "Garage")]/../div[@class="value"]/text()').extract())
        outdoor_comfort = response.xpath('.//div[contains(text(), "Confort extérieur")]/../div[@class="value"]/text()').extract_first()
        address = ''.join(response.xpath('.//div[contains(text(), "Adresse")]/../div[@class="value"]/text()').extract())
        zipcode, city = extract_city_zipcode(address)

        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_xpath('title', './/div[contains(@class, "fluid header")]//*[contains(@class, "leftside")]/text()')
        item_loader.add_xpath('address', './/div[contains(text(), "Adresse")]/../div[@class="value"]/text()')
        item_loader.add_xpath('rent_string', './/div[contains(@class, "fluid header")]//*[contains(@class, "rightside")]/text()')
        item_loader.add_xpath('utilities', './/div[contains(text(), "Charges locataire")]/../div[@class="value"]/text()')
        item_loader.add_xpath('description', './/div[contains(text(), "Description")]/..//div[@class="field"]/text()')
        item_loader.add_xpath('square_meters', './/div[contains(text(), "Superficie totale")]/../div[@class="value"]/text()')

        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        item_loader.add_xpath('images', './/a[contains(@href, "/pictures/Beaufils/xlarge/")]/@href')
        bathroom_count = response.xpath('.//div[contains(., "Nombre de salle(s) de bain") or contains(., "Nombre de salle(s) de douche")]/../div[@class="value"]/text()').extract_first()
        if bathroom_count:
            if extract_number_only(bathroom_count) != "0":
                item_loader.add_xpath('bathroom_count', extract_number_only(bathroom_count))
        item_loader.add_xpath('energy_label', './/div[contains(text(), "Certificat energétique")]/..//*[contains(text(), "totale")]/../../div[@class="value"]/text()')
        room_count = response.xpath('.//div[contains(text(), "Nombre de Chambre")]/../div[@class="value"]/text()').extract_first()
        if room_count:
            if extract_number_only(room_count) != "0":
                item_loader.add_xpath('room_count', extract_number_only(room_count))
        item_loader.add_value('landlord_name', 'IMMOBILIERE BEAUFILS')
        item_loader.add_value('landlord_email', 'info@beaufils.be')
        item_loader.add_value('landlord_phone', '067 84 16 60')
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)
        if parking:
            item_loader.add_value('parking', True)
        if outdoor_comfort and "Balcon" in outdoor_comfort:
            item_loader.add_value('balcony', True)
        if outdoor_comfort and "Terrasse" in outdoor_comfort:
            item_loader.add_value('terrace', True)
                 
        lat_lng = response.xpath("//iframe[@id='streetViewFrame']/@src[contains(.,'&sll=')]").get()
        if lat_lng:
            lat = lat_lng.split("&sll=")[1].split(",")[0].strip()
            lng = lat_lng.split("&sll=")[1].split(",")[1].split("&")[0]  
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Beaufils_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
