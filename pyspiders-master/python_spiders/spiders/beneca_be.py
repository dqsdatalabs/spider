# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from datetime import datetime
from math import ceil
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_rent_currency, format_date, string_found
from scrapy import Request
from datetime import date


class BenecaSpider(scrapy.Spider):
    name = 'beneca_be'
    allowed_domains = ['www.beneca.be']
    start_urls = ['http://www.beneca.be/te-huur']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    position = 0
    thousand_separator = '.'
    scale_separator = ','

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.beneca.be/te-huur/woningen/pagina-1',
            'property_type': 'house'
            },
            {
            'url': 'https://www.beneca.be/te-huur/appartementen/pagina-1',
            'property_type': 'apartment'
            }
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'page': 1,
                                'response_url': url.get('url'),
                                'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[@class="property"]')
        for listing in listings:
            url = listing.xpath('./@href').extract_first()
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
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_id', response.meta.get('response_url').split('/')[-1])

        # Get the title of the listing
        # item_loader.add_xpath('title', './/div[@class="property_meta"]//h1/text()')
        item_loader.add_xpath('title', './/meta[@property="og:title"]/@content')

        # get the description of the listing
        item_loader.add_xpath('description', './/div[contains(@class,"property_desc")]/div[not (contains(@class,"property__rep"))]/text()')

        # get the address of the listing
        address = response.xpath('.//p[@class="city"]/text()').extract_first()
        item_loader.add_xpath('address', './/p[@class="city"]/text()')
        # use the address to get the city of the listing
        if address and len(address.split(' ')) > 2:
            item_loader.add_value('city', address.split(' ')[-1])
            item_loader.add_value('zipcode', address.split(' ')[-2])

        item_loader.add_value('latitude', response.xpath('.//div[@class="property_map"]/script/text()').re(r'const lat = (.*); const')[0])
        item_loader.add_value('longitude', response.xpath('.//div[@class="property_map"]/script/text()').re(r'const lng = (.*);')[0])
        
        # get the number of rooms available if mentioned
        item_loader.add_xpath('room_count', './/li[@class="rooms"]/span/text()')
        item_loader.add_xpath('bathroom_count', './/li[@class="bathrooms"]/span/text()')

        item_loader.add_xpath('images', './/a[@class="lightGallery"]/@href')
        item_loader.add_value('property_type', response.meta.get('property_type'))
        
        item_loader.add_xpath('rent_string', './/dt[contains(text(), "Prijs")]/following-sibling::dd/text()')
        item_loader.add_xpath('utilities', './/dt[contains(text(), "Kosten")]/following-sibling::dd/text()')
        elevator = response.xpath('.//dt[contains(text(), "Lift")]/following-sibling::dd/text()').extract_first()
        if elevator:
            if elevator.lower() not in ['nee','0']:
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)

        # https://www.beneca.be/eengezinswoning-te-huur-in-meeuwen-gruitrode/4234091
        terrace = response.xpath('.//dt[contains(text(), "Terras")]/following-sibling::dd/text()').extract_first()
        if terrace:
            if terrace.lower() not in ['nee','0']:
                item_loader.add_value('terrace', True)
            else:
                item_loader.add_value('terrace', False)
        # parking
        parking = response.xpath('.//dt[contains(text(), "parking")]/following-sibling::dd/text()').extract_first()
        if parking:
            if parking.lower() not in ['nee','0']:
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)

        # balcony
        balcony = response.xpath('.//dt[contains(text(), "Balkon")]/following-sibling::dd/text()').extract_first()
        if balcony:
            if balcony.lower() not in ["nee", '0']:
                item_loader.add_value('balcony', True)
            else:
                item_loader.add_value('balcony', False)

        # furnished
        furnished = response.xpath('.//dt[contains(text(), "Gemeubeld")]/following-sibling::dd/text()').extract_first()
        if furnished:
            if furnished.lower() not in ["nee", '0']:
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)

        # square_meters
        item_loader.add_xpath('square_meters', './/dt[contains(text(), "Bewoonbare opp.")]/../dd/text()')
        item_loader.add_xpath('square_meters', './/dt[contains(text(), "Woonkamer")]/../dd/text()')
        item_loader.add_xpath('square_meters', './/dt[contains(text(), "Perceel opp.")]/../dd/text()')
        # available_date
        available_date = "".join(response.xpath('.//dt[contains(text(), "Beschikbaarheid")]/../dd/text()').extract())
        if "onmiddellijk" in available_date.lower():
            available_date = date.today().strftime("%d-%m-%Y")
        if "Mits inachtneming huurders" in available_date:
            available_date = response.xpath('.//dt[contains(text(), "Beschikbaarheid")]/../dd/text()[2]').extract_first()
            available_date = datetime.strptime(available_date, '%d %B %Y').strftime("%d-%m-%Y")
        if available_date and string_found(['in onderling overleg', 'bij oplevering'], available_date) is False:
            item_loader.add_value('available_date', format_date(available_date, "%d-%m-%Y"))

        # energy_label
        item_loader.add_xpath('energy_label', './/dt[contains(text(), "EPC") and contains(text(), "code")=False]/../dd/text()')
        item_loader.add_xpath('floor_plan_images', './/li[@class="property_download"]//span[contains(text(), "Plannen")]/../@href')

        item_loader.add_xpath('landlord_name', './/p[@class="rep__name"]/text()')
        item_loader.add_xpath('landlord_phone', 'substring-after(//p[@class="rep__phone"]/text(),": ")')
        item_loader.add_value('landlord_email', 'info@beneca.be')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Beneca_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
