# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

from datetime import datetime
import scrapy
# import locale as locale_lib
from ..loaders import ListingLoader
from scrapy import Request
from ..helper import string_found, format_date
import re


class MaximeSpider(scrapy.Spider):
    name = 'maxime_realestate_be'
    allowed_domains = ['www.maxime-realestate.be']
    start_urls = ['https://www.maxime-realestate.be/nl/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    available_date_locale_map = {"januari": "January", "februari": "February", "maart": "March", "april": "April",
                                 "mei": "May", "juni": "June", "juli": "July", "augustus": "August",
                                 "september": "September", "oktober": "October", "november": "November",
                                 "december": "December"}
    position = 0
    thousand_separator = '.'
    scale_separator = ','

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.maxime-realestate.be/nl/te-huur/appartementen/pagina-1',
            'property_type': 'apartment'
            },
            {
            'url': 'https://www.maxime-realestate.be/nl/te-huur/woningen/pagina-1',
            'property_type': 'house'
            },
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'page': 1,
                                'response_url': url.get('url'),
                                'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//li[contains(@class,"property")]/a[2]/@href').extract()
        for listing in listings:

            url = listing

            if url not in ['https://www.maxime-realestate.be/nl/properties/references', None]:
                yield scrapy.Request(url=url,
                                     callback=self.get_property_details,
                                     meta={'response_url': url,
                                           'property_type': response.meta.get('property_type')})

        if len(listings) > 0:
            next_page_url = response.meta.get('response_url')[:-1]+str(response.meta.get('page')+1)
            yield Request(url=next_page_url,
                          callback=self.parse,
                          meta={'page': response.meta.get('page')+1,
                                'response_url': next_page_url,
                                'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):

        item_loader = ListingLoader( response=response)

        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_id', response.meta.get('response_url').split('/')[-1])

        # Get the title of the listing
        item_loader.add_xpath('title', './/span[@class="desc"]/text()')
        # get the description of the listing
        item_loader.add_xpath('description', './/div[@class="desc-content"]/text()')
        # get the address of the listing
        item_loader.add_xpath('address', './/div[@id="information"]/div[1]/span[2]/text()')
        # use the address to get the city and zipcode of the listing
        item_loader.add_value('city', item_loader.get_output_value('address').split(' ')[-1])
        item_loader.add_value('zipcode', item_loader.get_output_value('address').split(' ')[-2])
        
        item_loader.add_xpath('rent_string', './/span[@class="price"]/text()')

        item_loader.add_xpath('square_meters', './/li[@class="area"]/text()')

        # get the number of rooms available if mentioned
        item_loader.add_xpath('room_count', './/li[@class="rooms"]/text()')
        item_loader.add_xpath('bathroom_count', './/li[@class="bathrooms"]/text()')

        item_loader.add_xpath('images', './/figure/a/@href')

        item_loader.add_value('property_type', response.meta.get('property_type'))

        terrace = response.xpath('.//dt[contains(text(), "terras")]/../dd/text()').extract_first()
        if terrace:
            if terrace.lower() not in ['nee', 0]:
                item_loader.add_value('terrace', True)
            else:
                item_loader.add_value('terrace', False)

        elevator = response.xpath('.//dt[contains(text(), "lift")]/../dd/text()').extract_first()
        if elevator:
            if elevator.lower() not in ['nee', 0]:
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)

        parking = response.xpath('.//dt[contains(text(), "garages") or contains(text(),"parking")]/../dd/text()').extract_first()
        if parking:
            if parking.lower() not in ['nee', 0]:
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)

        furnished = response.xpath('.//dt[contains(text(), "gemeubeld")]/../dd/text()').extract_first()
        if furnished:
            if furnished.lower() not in ['nee', 0]:
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)

        swimming_pool = response.xpath('.//dt[contains(text(), "zwembad")]/../dd/text()').extract_first()
        if swimming_pool:
            if swimming_pool.lower() not in ['nee', 0]:
                item_loader.add_value('swimming_pool', True)
            else:
                item_loader.add_value('swimming_pool', False)

        # item_loader.add_xpath('energy_label', './/dt[contains(text(), "epc")]/../dd/text()')
        item_loader.add_xpath('energy_label', './/dt[contains(text(), "epc")]/../dd[contains(text(), "kWh/m²")]/text()')
        item_loader.add_xpath('utilities', './/dt[contains(text(), "gemeenschappelijke kosten")]/../dd/text()')

        available_date = response.xpath('.//dt[contains(text(),"beschikbaarheid")]/../dd/text()').extract_first()
        if available_date:
            for key_i in self.available_date_locale_map:
                available_date = available_date.replace(key_i, self.available_date_locale_map[key_i])

            if "Mits inachtneming huurders" in available_date:
                available_date = response.xpath('.//dt[contains(text(), "beschikbaarheid")]/../dd/text()[2]').extract_first()
                for key_i in self.available_date_locale_map:
                    available_date = available_date.replace(key_i, self.available_date_locale_map[key_i])
                available_date = datetime.strptime(available_date, '%d %B %Y').strftime("%Y-%m-%d")
            if available_date and string_found(['in onderling overleg', 'bij oplevering', 'onmiddellijk'], available_date) is False:
                item_loader.add_value('available_date', format_date(available_date, "%Y-%m-%d"))
        elif re.findall(r'(?<=Beschikbaar vanaf )(\d{1,2}\/\d{1,2}\/\d{4})',item_loader.get_output_value('description')):
            available_date = re.findall(r'(?<=Beschikbaar vanaf )(\d{1,2}\/\d{1,2}\/\d{4})',item_loader.get_output_value('description'))
            item_loader.add_value('available_date', format_date(available_date[0], "%d/%m/%Y"))

        if response.xpath('.//div[@class="map-container "]/script/text()').extract_first() is not None:
            item_loader.add_value('latitude', response.xpath('//div[@class="map-container "]/script/text()').re(r'const lat = (.*); const')[0])
            item_loader.add_value('longitude', response.xpath('//div[@class="map-container "]/script/text()').re(r'const lng = (.*);')[0])

        item_loader.add_xpath('landlord_name', './/p[@class="rep-name"]/text()')
        item_loader.add_xpath('landlord_phone', './/p[@class="rep-mobile"]/text()')
        item_loader.add_xpath('landlord_email', './/p[@class="rep-email"]/text()')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "MaximeRealestate_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
