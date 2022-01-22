# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
# import js2xml
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
import re


class MannersresidentialSpider(scrapy.Spider):
    name = 'mannersresidential_com'
    allowed_domains = ['mannersresidential.com']
    start_urls = ['https://www.mannersresidential.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    position = 0
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.mannersresidential.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Detached&showstc=on',
            'property_type': 'house'},
            {
            'url': 'https://www.mannersresidential.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Maisonette&showstc=on',
            'property_type': 'house'},
            {
            'url': 'https://www.mannersresidential.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Semi-Detached&showstc=on',
            'property_type': 'house'},
            {
            'url': 'https://www.mannersresidential.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Terraced&showstc=on',
            'property_type': 'house'},
            {
            'url': 'https://www.mannersresidential.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat&showstc=on',
            'property_type': 'apartment'}
            ]

        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'page': 1,
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="row row-sml property"]')
        for property_item in listings:
            property_url = 'https://www.mannersresidential.com'+property_item.xpath('.//a[contains(text(),"MORE DETAILS")]/@href').extract_first()
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'room_count': property_item.xpath('//p[@class="bed-text"]/text()').extract()[0],
                      'bathroom_count': property_item.xpath('//p[@class="bed-text"]/text()').extract()[1],
                      'property_type': response.meta.get('property_type')}
            )

        if len(listings) == 10:
            next_page_url = None
            prev_url = response.meta.get('request_url').split('search/')
            if response.meta.get('page') == 1:
                next_page_url = prev_url[0] + 'search/' + str(response.meta.get('page')+1) + '.html?' + prev_url[-1]
            else:
                next_page_url = prev_url[0] + 'search/' + str(response.meta.get('page')+1) + '.html?' + prev_url[-1].split('?')[-1]

            if next_page_url:
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse,
                    meta={
                        'page': response.meta.get('page')+1,
                        'request_url': next_page_url,
                        'property_type': response.meta.get('property_type')}
                )

    def get_property_details(self, response):
        bathroom_count = response.meta.get('bathroom_count')
        rent_string = response.xpath('.//span[@class="pink-text uppercase-text"]/../text()').extract_first().split('pcm')[0]
        description = "".join(response.xpath('.//h2[contains(text(),"Description")]/../p/text()').extract())
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Mannersresidential_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').split('/')[-3])
        item_loader.add_value('property_type', response.meta.get('property_type'))
        
        title = "".join(response.xpath("//h1[@class='h2']//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
                
        room_count = response.meta.get('room_count')
        if room_count and int(room_count) != 0:
            item_loader.add_value('room_count', room_count)
        elif 'one bedroom' in description.lower() or 'one double bedroom' in description.lower():
            item_loader.add_value('room_count', '1')
        elif 'two bedroom' in description.lower():
            item_loader.add_value('room_count', '2')
        elif 'studio' in description.lower():
            item_loader.add_value('room_count', '1')
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('address', './/h1[@class="h2"]/text()')
        if item_loader.get_output_value('address'):
            city = item_loader.get_output_value('address').split(', ')[-1]
            item_loader.add_value('city', city)
        item_loader.add_xpath('images', './/div[@id="prop-slideshow"]//img/@src')
        item_loader.add_value('description', remove_unicode_char(description))
        floor_plan_images = response.xpath('.//div[@id="floorplan"]/img/@src').extract()
        for plan in floor_plan_images:
            item_loader.add_value('floor_plan_images', 'https://www.mannersresidential.com'+plan)
        item_loader.add_value('landlord_name', 'Manners Residential')
        item_loader.add_value('landlord_phone', '01483 590 059')
        item_loader.add_xpath('landlord_email', './/a[contains(@href,"mailto:")]/text()')

        latlng = response.xpath('.//script[contains(text(),"ShowMap")]/text()').extract_first()
        pos = re.search(r'q=(.+?)"\);', latlng)
        if pos:
            location = pos.group(1).split('%2C')
            item_loader.add_value('latitude', location[0])
            item_loader.add_value('longitude', location[1])
        
        self.position += 1
        item_loader.add_value('position', self.position)
        
        status = response.xpath("//span[contains(@class,'pink-text')]/text()").get()
        if status and "to let" in status.lower():
            yield item_loader.load_item()
