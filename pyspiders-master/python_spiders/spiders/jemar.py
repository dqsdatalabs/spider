# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from datetime import datetime
from ..loaders import ListingLoader
from ..helper import extract_rent_currency
import scrapy
from scrapy import Request
from scrapy.loader import ItemLoader
import re


class JemarSpider(scrapy.Spider):
    name = 'jemar_be'
    allowed_domains = ['www.jemar.be']
    start_urls = ['http://www.jemar.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.jemar.be/nl/te-huur?ptype=2&goal=1&page=1',
            'property_type': 'apartment'
            },
            {
            'url': 'https://www.jemar.be/nl/te-huur?ptype=3&goal=1&page=1',
            'property_type': 'studio'
            },
            {
            'url': 'https://www.jemar.be/nl/te-huur?ptype=1&goal=1&page=1',
            'property_type': 'house'
            }
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'page':1,
                                'response_url': url.get('url'),
                                'property_type':url.get('property_type')})

    def parse(self, response, **kwargs):

        listings = response.xpath('.//a[contains(@href, "id=")]/@href').extract()
        for url in listings:
            yield scrapy.Request(url=response.urljoin(re.sub(r"&page=\d+", "", url)),
                                 callback=self.get_property_details,
                                 meta={'response_url': response.urljoin(re.sub(r"&page=\d+", "", url)),
                                       'property_type': response.meta.get('property_type')})

        if len(listings) == 12:
            next_page_url = response.meta.get('response_url')[:-1]+str(response.meta.get('page')+1)
            yield Request(url=next_page_url,
                          callback=self.parse,
                          meta={'page': response.meta.get('page')+1,
                                'response_url': next_page_url,
                                'property_type':response.meta.get('property_type')})

    def get_property_details(self, response):
        
        item_loader = ListingLoader( response=response)

        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_id', response.meta.get('response_url').split('id=')[1].split('&')[0])

        # Get the title of the listing
        item_loader.add_xpath('title', '//div[@id="PropertyRegion"]//h3/text()')

        item_loader.add_value('property_type', response.meta.get('property_type'))

        # get the description of the listing
        item_loader.add_xpath('description', '//div[@class="content descr"]/div/p/span/text()')

        # images
        # item_loader.add_xpath('images', '//div[@class="swiper-container gallery-top-mobile"]//div[contains(@class,"swiper-slide")]/img/@src')
        item_loader.add_xpath('images', './/div[@class="showmobile"]//div[@class="swiper-slide"]/img/@data-src')

        # bed rooms
        item_loader.add_xpath('room_count', '//div[contains(text(),"Aantal slaapkamers")]/../div[@class="value"]/text()')

        # bath rooms
        item_loader.add_xpath('bathroom_count', '/div[contains(text(),"Aantal badkamers")]/../div[@class="value"]/text()')

        item_loader.add_xpath('address', '//div[contains(text(),"Adres")]/../div[@class="value"]/text()')
        item_loader.add_value('city', item_loader.get_output_value('address').split(' ')[-1])
        item_loader.add_value('zipcode', item_loader.get_output_value('address').split(' ')[-2])

        item_loader.add_xpath('square_meters', '//div[contains(text(),"Bewoonbare opp.")]/../div[@class="value"]/text()')

        available_date = response.xpath('//div[contains(text(),"Beschikbaarheid")]/../div[@class="value"]/text()').extract_first()
        if available_date :
            if available_date.lower() == 'onmiddellijk':
                item_loader.add_value('available_date', datetime.now().strftime("%Y-%m-%d"))
            elif 'Huurovereenkomst te respecteren' not in available_date:
                item_loader.add_value('available_date', datetime.strptime(available_date, '%d/%m/%y').strftime("%Y-%m-%d"))

        parking = response.xpath('//div[contains(text(),"Garage")]/../div[@class="value"]/text()').extract_first()
        if parking:
            if parking.lower() not in ["nee", 0]:
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)

        elevator = response.xpath('//div[contains(text(),"Lift")]/../div[@class="value"]/text()').extract_first()
        if elevator:
            if elevator.lower() not in ["nee", 0]:
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)
        energy_label = response.xpath('.//div[contains(text(), "E")]/sub[contains(text(), "spec")]/../../div[@class="value"]/text()').extract_first()
        if energy_label and energy_label not in ["In aanvraag"]:
            item_loader.add_value('energy_label', energy_label)
        # latitude and longitude
        if response.xpath('//*[@id="streetViewFrame"]/@src').extract_first():
            latlong = response.xpath('//*[@id="streetViewFrame"]/@src').extract_first()
            pos = re.search(r'sll=.+?&', latlong)
            if pos:
                lat = round(float(latlong[int(pos.start())+4:int(pos.end())].split(',')[0]), 5)
                long = round(float(latlong[int(pos.start()):int(pos.end())-1].split(',')[1]), 5)
                item_loader.add_value('latitude', str(lat))
                item_loader.add_value('longitude', str(long))

        # Rent
        item_loader.add_xpath('rent_string', '//div[contains(text(),"Prijs")]/../div[@class="value"]/text()')
        item_loader.add_xpath('utilities', './/div[contains(text(), "provisie")]/../div[@class="value"]/text()')

        # landlord information
        item_loader.add_value('landlord_phone', '089/77.23.20')
        item_loader.add_value('landlord_email', 'info@jemar.be')
        item_loader.add_value('landlord_name', 'Jemar')
        
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Jemar_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
    

