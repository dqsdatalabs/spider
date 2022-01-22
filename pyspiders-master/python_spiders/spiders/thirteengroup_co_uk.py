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
from math import ceil
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent


class ThirteengroupSpider(scrapy.Spider):
    name = 'thirteengroup_co_uk'
    allowed_domains = ['thirteengroup.co.uk']
    start_urls = ['https://www.thirteengroup.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.thirteengroup.co.uk/properties?location=&tenures=1&keywords=&min_sale_price=0&max_sale_price=260000&rent_duration=week&min_weekly_rent=0&max_weekly_rent=165&min_monthly_rent=0&max_monthly_rent=720&min_beds=1&max_beds=5&types=5&search=1',
                'property_type': 'house'},
            {
                'url': 'https://www.thirteengroup.co.uk/properties?location=&tenures=1&keywords=&min_sale_price=0&max_sale_price=260000&rent_duration=week&min_weekly_rent=0&max_weekly_rent=165&min_monthly_rent=0&max_monthly_rent=720&min_beds=1&max_beds=5&types=2&search=1',
                'property_type': 'apartment'},
            {
                'url': 'https://www.thirteengroup.co.uk/properties?location=&tenures=1&keywords=&min_sale_price=0&max_sale_price=260000&rent_duration=week&min_weekly_rent=0&max_weekly_rent=165&min_monthly_rent=0&max_monthly_rent=720&min_beds=1&max_beds=5&types=1&search=1',
                'property_type': 'house'},
            {
                'url': 'https://www.thirteengroup.co.uk/properties?location=&tenures=1&keywords=&min_sale_price=0&max_sale_price=260000&rent_duration=week&min_weekly_rent=0&max_weekly_rent=165&min_monthly_rent=0&max_monthly_rent=720&min_beds=1&max_beds=5&types=7&search=1',
                'property_type': 'apartment'
            }
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'request_url': url.get("url"),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="image"]//a/@href').extract()
        for property_item in listings:
            yield scrapy.Request(
                url=f"https://www.thirteengroup.co.uk/{property_item}",
                callback=self.get_property_details,
                meta={'request_url': f"https://www.thirteengroup.co.uk/{property_item}",
                      'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        external_id = response.meta.get('request_url').split("/")[-1]
        if all(char.isnumeric() for char in external_id):
            item_loader.add_value('external_id', external_id)
        item_loader.add_xpath('title', './/span[@class="page-title-wrapper"]/text()')
        address = response.xpath('.//span[@class="page-summary-wrapper"]/text()').extract_first()
        if address:
            item_loader.add_value('address', address)
            city_zip = address.split(',')
            item_loader.add_value('city', city_zip[-2])
            item_loader.add_value('zipcode', city_zip[-1])

        item_loader.add_xpath('description', './/div[@id="body-main-middle"]//p/text()')
        item_loader.add_xpath('images', './/a[@data-fancybox="property_images"]/img/@src')
        item_loader.add_value('landlord_name', 'Thirteen Group')
        item_loader.add_value('landlord_email', 'customerservices@thirteengroup.co.uk')

        tel = response.xpath('.//p[@class="property-phone"]/text()').extract_first()
        if tel:
            tel_cleaned = tel.split("Call ")[-1]
            item_loader.add_value('landlord_phone', tel_cleaned)
        else:
            item_loader.add_xpath('landlord_phone', '0300 111 1000')

        rent_string = response.xpath('.//span[contains(text(), "per month")]/text()').extract_first()
        if rent_string:
            item_loader.add_value('rent_string', rent_string)
        else:
            rent_string = response.xpath('.//li[contains(text(), "per week")]/text()').extract_first()
            item_loader.add_value('rent_string', "£ " + str(extract_rent_currency(rent_string, ThirteengroupSpider)[0] * 4))

        energy = response.xpath('.//li[contains(text(),"Energy")]/text()').extract_first()
        if energy:
            label = energy.split(":")[-1]
            if label:
                item_loader.add_value('energy_label', label)

        map_cords_url = response.xpath('.//a[@class="thirteen-property-map-link"]/@href').extract_first()
        if map_cords_url:
            map_cords = map_cords_url.split("q=")[-1].split(',')
            if map_cords[0] != '0' and map_cords[1] != '0':
                item_loader.add_value('latitude', map_cords[0])
                item_loader.add_value('longitude', map_cords[1])

        room = response.xpath('.//th[contains(text(),"Max Bed")]/following::td/text()').extract_first()
        if room:
            item_loader.add_value('room_count', room)
        else:
            room_element = response.xpath('.//li[contains(text(),"bedrooms") or contains(text(),"bedroom")]/text()').extract_first()
            item_loader.add_value('room_count', extract_number_only(room_element))

        # pets_allowed
        pets_allowed = response.xpath('.//th[contains(text(), "Pets Permitted")]/../td/text()').extract_first()
        if pets_allowed and pets_allowed.lower() == "yes":
            item_loader.add_value('pets_allowed', True)
        elif pets_allowed and pets_allowed.lower() == "No":
            item_loader.add_value('pets_allowed', False)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Thirteengroup_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
