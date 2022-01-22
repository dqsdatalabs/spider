# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy
# from scrapy.http import FormRequest
import re
from ..loaders import ListingLoader
from ..helper import format_date


class EuRentals(scrapy.Spider):

    name = 'eu_rentals_com'
    allowed_domains = ['eu-rentals.com']
    start_urls = ['https://www.eu-rentals.com']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = ["https://www.eu-rentals.com/apartments-for-rent/list?page=0"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'page': 0,
                                       'request_url': url})
    
    def parse(self, response, **kwargs):
        listings = response.xpath('.//h2/a[contains(@href,"/apartment/")]/@href').extract()
        # '//div[@class="property-container"]//a[contains(@href, "/city/")]/text()'
        for url in listings:
            url = response.urljoin(re.sub(r"page=\d+", "", url))
            yield scrapy.Request(url=url,
                                 callback=self.get_property_details,
                                 meta={'request_url': url})

        if len(listings) == 6:
            next_page_url = response.meta.get('request_url')[:-1] + str(response.meta.get('page')+1)
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse,
                                 meta={'page': response.meta.get('page')+1,
                                       'request_url': next_page_url})
    
    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_xpath('external_id', './/h3[contains(text(),"Property unique ID")]/../div/div/text()')
        item_loader.add_xpath('title', './/h1[@id="page-title"]/text()')
        item_loader.add_xpath('property_type', './/div[@class="summary"]/div[contains(@class,"property-type")]/text()')
        item_loader.add_xpath('rent_string', './/h3[contains(text(), "Price:")]/../text()')
        available_date = response.xpath('.//div[@class="field field-name-field-available-from"]/text()').extract_first()
        item_loader.add_value('available_date', format_date(available_date))
        item_loader.add_xpath('room_count', './/h3[contains(text(),"Bedrooms")]/../div/div/text()')
        item_loader.add_xpath('bathroom_count', './/h3[contains(text(),"Bathrooms")]/../div/div/text()')
        item_loader.add_xpath('square_meters', './/h3[contains(text(),"Surface")]/../div/div/text()')
        item_loader.add_xpath('description', './/h3[contains(text(),"Description")]/..//p/text()')
        item_loader.add_xpath('landlord_name', './/a[contains(@href, "/property-manager")]/text()')
        item_loader.add_xpath('landlord_phone', './/a[contains(@href, "tel")]//text()')
        item_loader.add_xpath('images', './/div[@id="image-slider"]//img[contains(@src, "/sites/default/files")]/@src')

        city = response.xpath('//div[@id="image-slider"]//img[contains(@src, "/sites/default/files")]/@title').extract_first()
        if city and len(city) > 1:
            item_loader.add_value('city', city.split(', ')[1])

        # Trying to parse js to xml Gives syntax error in the javascript and doesnt allow parsing

        javascript = response.xpath('.//script[contains(text(),"lat") and contains(text(),"lon")]/text()').extract_first()
        if javascript:
            lat = re.findall(r'"lat":([\d\.]+?),', javascript)
            lon = re.findall(r'"lon":([\d\.]+?)}', javascript)

            if len(lon) > 0 and len(lat) > 0:
                item_loader.add_value('latitude', lat[0])
                item_loader.add_value('longitude', lon[0])

        # https://www.eu-rentals.com/apartment/10138
        balcony = response.xpath('//li[@class="balcony"]').extract_first()
        if balcony:
            item_loader.add_value('balcony', True)
        
        # https://www.eu-rentals.com/apartment/10138
        furnished = response.xpath('//li[@class="furnished"]').extract_first()
        if furnished:
            item_loader.add_value('furnished', True)
        
        terrace = response.xpath('//li[@class="terrace"]').extract_first()
        if terrace:
            item_loader.add_value('terrace', True)
        
        # https://www.eu-rentals.com/apartment/9399
        elevator = response.xpath('//li[@class="elevator"]').extract_first()
        if elevator:
            item_loader.add_value('elevator', True)

        # https://www.eu-rentals.com/apartment/10138
        dishwasher = response.xpath('//li[@class="dishwasher"]').extract_first()
        if dishwasher:
            item_loader.add_value('dishwasher', True)
        
        # https://www.eu-rentals.com/apartment/10138
        washing_machine = response.xpath('//li[@class="washing-machine---dryer"]').extract_first()
        if washing_machine:
            item_loader.add_value('washing_machine', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "EuRentals_PySpider_{}_{}".format(self.country, self.locale))
        return item_loader.load_item()
