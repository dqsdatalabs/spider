# -*- coding: utf-8 -*-
# Author: Praveen Chaudhary
# Team: Sabertooth
import re

import scrapy

from ..loaders import ListingLoader


class CoopersaqsComPyspiderUnitedkingdomEnSpider(scrapy.Spider):
    name = 'coopersaqs_com'
    allowed_domains = ['coopersaqs.com']
    start_urls = ['https://coopersaqs.com/property-status/for-rent/'
                  ]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        self.position = 0
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="rh_figure_property_list_one"]/a/@href').extract()
        for property_item in listings:
            yield scrapy.Request(
                url=property_item,
                callback=self.get_property_details,
                meta={'request_url': property_item, }
            )
        next_page_url = response.xpath('.//a[contains(@class,"rh_pagination__next")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_xpath('external_id', './/p[@class="id"]/text()')
        item_loader.add_xpath('title', './/title/text()')

        item_loader.add_xpath('rent_string', './/p[@class="price"]/text()')
        item_loader.add_xpath('description', './/div[@class="rh_content"]/p/text()')
        if "Apartment" in item_loader.get_output_value('description'):
            item_loader.add_value('property_type', "apartment")
        elif "house" in item_loader.get_output_value('description'):
            item_loader.add_value('property_type', "house")
        else:
            item_loader.add_value('property_type', "studio")
        item_loader.add_xpath('images', './/ul[@class="slides"]//img/@src')
        item_loader.add_value('landlord_name', 'Cooper Saqs')
        item_loader.add_value('landlord_email', 'info@coopersaqs.com')
        item_loader.add_value('landlord_phone', '020 3439 0000')
        item_loader.add_xpath('bathroom_count', './/div[contains(@class,"prop_bedrooms")]/div/span/text()')
        item_loader.add_xpath('room_count', './/div[contains(@class,"prop_bathrooms")]/div/span/text()')

        address = response.xpath('.//h1[@class="rh_page__title"]/text()').extract_first().strip()

        if item_loader.get_output_value('description'):
            regex_pattern = r"situated|found in (?P<city>(\w+)) (?P<zipcode>(\w+)),"
            regex = re.compile(regex_pattern)
            match = regex.search(item_loader.get_output_value('description'))
            if match:
                item_loader.add_value('city', match["city"])
                address += f", {match['city']}"
            if match:
                item_loader.add_value('zipcode', match["zipcode"])
                address += f", {match['zipcode']}"
        item_loader.add_value('address', address)

        elevator = response.xpath('.//li[@class="rh_property__feature"]/a[contains(text(),"Elevator")]')
        if elevator:
            item_loader.add_value('elevator', True)

        parking = response.xpath('.//li[@class="rh_property__feature"]/a[contains(text(),"Parking")]')
        if parking:
            item_loader.add_value('parking', True)

        # ex https://coopersaqs.com/property/1-ebury-square/
        if response.xpath('.//li[@id="rh_property__feature_133"]'):
            item_loader.add_value('furnished', True)

        # ex https://coopersaqs.com/property/1-ebury-square/
        if response.xpath('.//li[@id="rh_property__feature_139"]'):
            item_loader.add_value('balcony', True)

        if response.xpath('.//li[@id="rh_property__feature_141"]'):
            item_loader.add_value('terrace', True)

        if response.xpath('.//li[@id="rh_property__feature_45"]'):
            item_loader.add_value('swimming_pool', True)

        self.position += 1
        item_loader.add_value('position', self.position)

        item_loader.add_value("external_source",
                              "CoopersaqsCom_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
