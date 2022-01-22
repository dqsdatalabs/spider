# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import format_date, extract_number_only, extract_rent_currency
from scrapy import Request
import re


class MyimmoSpider(scrapy.Spider):
    name = 'myimmo_be'
    allowed_domains = ['www.myimmo.be']
    start_urls = ['http://www.myimmo.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.myimmo.be/fr/location.php?typeFilter=1&p=1',
            'property_type': 'house',
            'type': "1"
            },
            {
            'url': 'https://www.myimmo.be/fr/location.php?typeFilter=2&p=1',
            'property_type': 'apartment',
            'type': "2"
            }
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'response_url': url.get('url'),
                                'property_type': url.get('property_type'),
                                'type': url.get('type')})

    def parse(self, response, **kwargs):
        page = response.meta.get('page', 2)
        seen=False
        listings = response.xpath('.//div[@class="texte"]/a/@href').extract()
        for url in listings:
            url = response.urljoin(url)
            yield scrapy.Request(url=url,
                                 callback=self.get_property_details,
                                 meta={'response_url': url,
                                       'property_type': response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:
            typeFilter = response.meta.get('type')
            f_url = f"https://www.myimmo.be/fr/location.php?p={page}&typeFilter={typeFilter}"
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type'), 'type': response.meta.get('type')})

        
        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_xpath('external_id', './/span[@class="ref"]/b/text()')

        item_loader.add_xpath('title', './/h1[@class="titreMain"]/text()')
        rent = response.xpath('.//div[@class="prix"]/text()').extract_first()
        if rent:
            rent = rent.replace("€","").strip().replace(".","")
        item_loader.add_value("currency", "EUR")
        item_loader.add_value('rent', rent)
        utilities = response.xpath('//li/span/b[contains(text(),"Charges mensuelles")]/../i/text()').get()
        if utilities:
            utilities = utilities.replace("€","").strip()
            item_loader.add_value("utilities", utilities)
        deposit = response.xpath('.//li/span/b[contains(text(),"Garantie location (nombre de mois) :")]/../i/text()').extract_first()
        if deposit and len(deposit) > 0:
            deposit = int(deposit)*int(rent)
            item_loader.add_value('deposit', deposit)

        room_count = response.xpath('.//div[@class="ch"]/span/text()').extract_first()
        if room_count:
            if extract_number_only(room_count) != '0':
                item_loader.add_value('room_count', extract_number_only(room_count))
            else:
                item_loader.add_value('room_count', '1')
                item_loader.add_value('property_type', 'studio')
        bathroom_count = response.xpath('.//div[@class="sdb"]/span/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))

        item_loader.add_xpath('square_meters', './/div[@class="int"]//text()')
        item_loader.add_xpath('description', './/h3[contains(text(),"Description")]/../p/text()')

        address = response.xpath('//title/text()').get()
        if address and address.count("-") > 1:
            item_loader.add_value('address', address.split()[0].strip())
        else:
            address = response.xpath('.//div[@class="adresse"]//text()').extract()
            if len(address) > 0:
                address = ", ".join(address)
                item_loader.add_value('address', address)
                if len(re.findall(r"(?<=\d)[^\d]+$", address)) > 0:
                    item_loader.add_value('city', re.findall(r"(?<=\d)[^\d]+$", address)[0].strip())
                if len(re.findall(r"\d{4,8}", address)) > 0:
                    item_loader.add_value('zipcode', re.findall(r"\d{4,8}", address)[0])
        
        item_loader.add_value('latitude', response.xpath('.//li/span/b[contains(text(),"Coordonnées xy (x) :")]/../i/text()').extract_first().replace(',','.'))
        item_loader.add_value('longitude', response.xpath('.//li/span/b[contains(text(),"Coordonnées xy (y) :")]/../i/text()').extract_first().replace(',','.'))
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_xpath('images', './/a[@data-fancybox="photo"]/@href')

        terrace = response.xpath('.//li/span/b[contains(text(),"Terrasse (nombre) :")]/../i/text()').extract_first()
        if terrace:
            if terrace not in [0, 'non']:
                item_loader.add_value('terrace', True)
            else:
                item_loader.add_value('terrace', False)

        elevator = response.xpath('.//li/span/b[contains(text(),"Ascenseur :")]/../i/text()').extract_first()
        if elevator:
            if elevator not in [0, 'non']:
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)

        parkings = response.xpath('.//li/span/b[contains(text(),"Garage (number):") or contains(text(),"Outdoor parking (number):") \
            or contains(text(),"Parking intérieur :")]/../i/text()').extract()
        for parking in parkings:
            if parking not in [0, 'non']:
                item_loader.add_value('parking', True)
                break
            else:
                item_loader.add_value('parking', False)

        item_loader.add_xpath('energy_label', './/li/span/b[contains(text(),"PEB (E spec (kwh / m² / year)):")]/../i/text()')

        item_loader.add_value('landlord_name', 'MyImmo')
        item_loader.add_xpath('landlord_phone', './/a[contains(@class,"tel")]/text()')
        item_loader.add_value('landlord_email', 'europa@myimmo.be')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Myimmo_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
