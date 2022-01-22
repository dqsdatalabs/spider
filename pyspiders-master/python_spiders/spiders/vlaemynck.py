# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from datetime import datetime

import scrapy
from scrapy import Request

from ..helper import format_date
from ..loaders import ListingLoader



class VlaemynckSpider(scrapy.Spider):
    name = 'vlaemynck_be'
    allowed_domains = ['www.vlaemynck.be']
    start_urls = ['http://www.vlaemynck.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    thousand_separator = ' '
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.vlaemynck.be/nl/te-huur/appartement/',
             'property_type': 'apartment'},
            {'url': 'https://www.vlaemynck.be/nl/te-huur/huis/',
             'property_type': 'house'},
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'response_url': url.get('url'),
                                'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listing_urls = response.xpath('//div[@id="divProperties"]//a/@href').extract()
        for listing_url in listing_urls:
            url = response.urljoin(listing_url)
            yield Request(url=url,
                          callback=self.get_property_details,
                          meta={'response_url': url,
                                'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_id', response.meta.get('response_url').split('/')[-3])
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_xpath('title', './/section[@class="info"]//h1/text()')

        if response.xpath('.//p[@class="location"]/text()'):
            item_loader.add_value('address', response.xpath('.//p[@class="location"]/text()').extract_first()[:-2])
            if any(char.isdigit() for char in item_loader.get_output_value('address').split(' ')[-1]):
                item_loader.add_value('city', item_loader.get_output_value('address').split(' ')[-2])
                item_loader.add_value('zipcode', item_loader.get_output_value('address').split(' ')[-3])
            else:
                item_loader.add_value('city', item_loader.get_output_value('address').split(' ')[-1])
                item_loader.add_value('zipcode', item_loader.get_output_value('address').split(' ')[-2])
        else:
            item_loader.add_xpath('city', './/th[contains(text(),"Stad/Gemeente")]/parent::tr/td/text()')
            province = response.xpath('.//th[contains(text(),"Provincie")]/../td/text()').extract_first()
            if province:
                item_loader.add_value('address', f"{item_loader.get_output_value('city')} ,{province}")

        item_loader.add_xpath('description', './/section[@class="info"]//div[@class="row"]//p/text()')

        item_loader.add_xpath('room_count', './/th[contains(text(),"Slaapkamer")]/../td/text()')
        item_loader.add_xpath('bathroom_count', './/th[contains(text(),"Badkamer")]/../td/text()')
        item_loader.add_xpath('energy_label', './/tr[@class="epc"]/td/text()')
        item_loader.add_xpath('square_meters', './/th[contains(text(),"Bewoonbare opp.")]/../td/text()')
        item_loader.add_xpath('square_meters', './/th[contains(text(),"Totale opp.")]/../td/text()')

        item_loader.add_xpath('utilities', './/th[contains(text(),"Maandelijkse lasten")]/../td/text()')

        elevator = response.xpath('.//th[contains(text(),"Lift")]/../td/text()').extract_first()
        if elevator and elevator.lower() not in ['0', 'nee']:
            item_loader.add_value('elevator', True)
        elif elevator:
            item_loader.add_value('elevator', False)

        terrace = response.xpath('.//th[contains(text(),"Terras")]/../td/text()').extract_first()
        if terrace and terrace.lower() not in ['0', 'nee']:
            item_loader.add_value('terrace', True)
        elif terrace:
            item_loader.add_value('terrace', False)

        parking = response.xpath('.//th[contains(text(),"Autostaanplaats")]/../td/text()').extract_first()
        garage = response.xpath("//th[contains(.,'Garage')]/following-sibling::td/text()[.!='0']").get()
        if parking and parking.lower() not in ['0', 'nee']:
            item_loader.add_value('parking', True)
        elif garage:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath('.//th[contains(text(),"Balkon")]/../td/text()').extract_first()
        if balcony and balcony.lower() not in ['0', 'nee']:
            item_loader.add_value('balcony', True)
        elif balcony:
            item_loader.add_value('balcony', False)

        available_date = response.xpath('.//th[contains(text(),"Beschikbaar vanaf")]/../td/text()').extract_first()
        months_in_dutch = ['januari', 'februari', 'maart', 'april', 'mei', 'juni', 'juli', 'augustus', 'september',
                           'oktober', 'november', 'december']
        if available_date and available_date.lower() == 'onmiddellijk vrij':
            item_loader.add_value('available_date', datetime.now().strftime("%Y-%m-%d"))
        else:
            regex_pattern = r"(?P<date>(\d+)) (?P<month>(\w+)) (?P<year>(\d+))"
            regex = re.compile(regex_pattern)
            match = regex.search(item_loader.get_output_value('description'))
            if match: 
                try:
                    available_date = f"{match['date']}/{months_in_dutch.index(match['month']) + 1}/{match['year']}"
                    item_loader.add_value('available_date', format_date(available_date))
                except: pass
        rent_string = response.xpath('.//span[@class="price"]/text()').extract_first()
        if rent_string:
            rent_string = rent_string.replace("&nbsp;","").split()
            item_loader.add_value('rent_string', rent_string)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'Vlaemynck')
        item_loader.add_xpath('landlord_phone', './/a[contains(@href,"tel:")]/text()')
        item_loader.add_xpath('images', ".//div[contains(@class,'owl-carousel')]//@src")

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Vlaemynck_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
