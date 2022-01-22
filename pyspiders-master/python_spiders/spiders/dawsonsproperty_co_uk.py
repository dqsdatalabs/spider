# -*- coding: utf-8 -*-
# Author: Praveen Chaudhary
# Team: Sabertooth

import lxml
import scrapy
import js2xml
import re

from scrapy import Selector

from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only, \
    convert_string_to_numeric
from datetime import date
from ..user_agents import random_user_agent


class DawsonspropertySpider(scrapy.Spider):
    name = 'dawsonsproperty_co_uk'
    allowed_domains = ['dawsonsproperty.co.uk']
    start_urls = ['https://dawsonsproperty.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://dawsonsproperty.co.uk/lettings.php?sort_by=recent&min-area=&property=Flat%2FApartment&minimum=0&maximum=999999999999&bedrooms=0',
                'property_type': 'apartment'},
            {
                'url': 'https://dawsonsproperty.co.uk/lettings.php?sort_by=recent&min-area=&property=Studio&minimum=0&maximum=999999999999&bedrooms=0',
                'property_type': 'studio'},
            {
                'url': 'https://dawsonsproperty.co.uk/lettings.php?sort_by=recent&min-area=&property=Bungalow&minimum=0&maximum=999999999999&bedrooms=0',
                'property_type': 'house'},
            {
                'url': 'https://dawsonsproperty.co.uk/lettings.php?sort_by=recent&min-area=&property=House+in+Multiple+Occupation&minimum=0&maximum=999999999999&bedrooms=0',
                'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"homes-content")]')
        for property_item in listings:
            property_url = property_item.xpath('.//a[contains(text(),"VIEW DETAILS")]/@href').extract_first()
            room_count = property_item.xpath('.//i[contains(@class,"bed")]/following-sibling::span/text()').extract_first()
            bathroom_count = property_item.xpath('.//i[contains(@class,"bath")]/following-sibling::span/text()').extract_first()
            yield scrapy.Request(
                url=f"https://dawsonsproperty.co.uk/{property_url}",
                callback=self.get_property_details,
                meta={'request_url': f"https://dawsonsproperty.co.uk/{property_url}",
                      'property_type': response.meta.get('property_type'),
                      'room_count':room_count,
                      'bathroom_count':bathroom_count}
            )

        next_page_url = response.xpath('.//a[contains(text(),"Next")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(f"https://dawsonsproperty.co.uk{next_page_url}"),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').split("-")[-1])

        if response.meta.get('room_count'):
            item_loader.add_value('room_count', extract_number_only( response.meta.get('room_count')))
        if response.meta.get('bathroom_count'):
            item_loader.add_value('bathroom_count', extract_number_only( response.meta.get('bathroom_count')))

        address_or_title = response.xpath('.//div[contains(@class,"block-heading")]/h1/text()').extract_first()
        if address_or_title:
            item_loader.add_value('title', address_or_title)
            item_loader.add_value('address', address_or_title)
            item_loader.add_value('city', address_or_title.split(",")[-2].strip())
            zipcode = address_or_title.split(",")[-1].strip()            
            item_loader.add_value('zipcode', f"{zipcode.split(' ')[-2]} {zipcode.split(' ')[-1]}")

        rent = response.xpath('.//div[@class="block-heading"]//em/text()').extract_first()
        item_loader.add_value('rent_string', f"£{rent}")

        item_loader.add_xpath('description', './/h5[contains(text(),"Full Description")]/following::p/text()')
        item_loader.add_xpath('images', './/div[@id="carouselExampleIndicators"]//img/@src')
        item_loader.add_value('landlord_name', 'Dawsons Property')
        item_loader.add_value('landlord_email','sw@dawsonsproperty.co.uk')
        item_loader.add_value('landlord_phone','01792 646060')
        map_cords_url = response.xpath('.//div[contains(@class,"location ")]/iframe/@src').extract_first()
        if map_cords_url:
            map_cords = map_cords_url.split("marker=")[-1].split("%2C")
            item_loader.add_value('latitude', map_cords[0])
            item_loader.add_value('longitude', map_cords[1])
        # room = response.xpath('.//i[contains(@class,"bed")]').extract()
        # bath = response.xpath('.//i[contains(@class,"bath")]').extract()    
        # if room:
        #     room_element = extract_number_only(room)
        #     if room_element:
        #         item_loader.add_xpath('room_count',room_element)
        # if bath:
        #     bath_element = extract_number_only(bath)
        #     if bath_element:
        #         item_loader.add_xpath('bathroom_count', bath_element)

        features = ' '.join(response.xpath('.//h5[contains(text(),"Features")]/following-sibling::h6//li/span/text()').extract())

        # https://dawsonsproperty.co.uk/lettings/robert-owen-gardens-port-tennant-swansea-sa1-8nr-r140
        if 'terrace' in features.lower():
            item_loader.add_value('terrace',True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Dawsonsproperty_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
