# -*- coding: utf-8 -*-
# Author: Praveen Chaudhary
# Team: Sabertooth

import scrapy
import re
import requests
from scrapy.http import HtmlResponse
from geopy.geocoders import Nominatim
from ..helper import extract_number_only, format_date
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class HamiltonsSalesSpider(scrapy.Spider):
    name = 'hamiltons_sales_co_uk'
    allowed_domains = ['hamiltons-sales.co.uk']
    start_urls = ['https://www.hamiltons-sales.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    page_no = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.hamiltons-sales.co.uk/lettings?filter_attribute[categorical][1]=House&q=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][3][min]=&min_price=',
                'property_type': 'house',
                'param': 'House'},
            {
                'url': 'https://www.hamiltons-sales.co.uk/lettings?filter_attribute[categorical][1]=Flat&q=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][3][min]=&min_price=',
                'property_type': 'apartment',
                'param': 'Flat'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'param': url.get('param'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="work-img"]//a/@href').getall()
        if listings:
            for property_item in listings:
                yield scrapy.Request(
                    url=f"https://www.hamiltons-sales.co.uk/{property_item}",
                    callback=self.get_property_details,
                    meta={'request_url': f"https://www.hamiltons-sales.co.uk/{property_item}",
                          'property_type': response.meta.get('property_type')}
                )
            self.page_no += 1
            yield scrapy.Request(
                url=f"https://www.hamiltons-sales.co.uk/lettings?filter_attribute[categorical][1]={response.meta.get('param')}&p={self.page_no}&q=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][3][min]=&min_price=&property_area=Long%20Let",
                callback=self.parse,
                meta={'param': response.meta.get('param'),
                      'property_type': response.meta.get('property_type')}
            )
        else:
            self.page_no = 0

    def get_property_details(self, response):
        id_title_string = response.meta.get('request_url').split("q=")[-1].split('-', 1)
        available_date = response.xpath('.//span[contains(@class,"available")]/span/text()').extract_first().split(", ")[-1]
        features = response.xpath('.//h3[contains(text(),"Features")]/following::div[@class="field-content"]/text()').extract_first()

        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', id_title_string[0])
        item_loader.add_value('title', id_title_string[-1])
        item_loader.add_xpath('rent_string', './/h1[contains(text(),"pcm")]/text()')
        item_loader.add_xpath('description', './/div[contains(@class,"description")]/p/text()')
        item_loader.add_xpath('images', './/div[@id="carousel"]//img/@src')

        # bathroom_count = response.xpath('.//span[contains(@class,"bathrooms")]/text()').extract_first()
        bathroom_count = response.xpath('.//span[contains(text(), "Bathroom")]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))
        # room_count = response.xpath('.//span[contains(@class,"bedrooms")]/text()').extract_first()
        room_count = response.xpath('.//span[contains(text(), "Bedroom")]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', extract_number_only(room_count))

        item_loader.add_value('landlord_name', 'Hamiltons Sales')
        item_loader.add_xpath('landlord_email', './/a[contains(@href,"mailto")]/text()')
        item_loader.add_xpath('landlord_phone', './/a[contains(@href,"tel")]/text()')
        item_loader.add_xpath('floor_plan_images', './/input[@id="floor_plan_1"]/@value')
        if available_date:
            # available_date = re.sub(r'(\d)(st|nd|rd|th)', r'\1', available_date)
            item_loader.add_value('available_date', format_date(available_date, "%d %b %Y"))

        maps_url = response.xpath('.//a[contains(@href,"maps?q=")]/@href').extract_first()
        if maps_url:
            temp_response = requests.get(maps_url, headers={"User-Agent": random_user_agent()})
            temp_response = HtmlResponse(url=maps_url, body=temp_response.text, encoding="utf-8")
            lat_lng = temp_response.xpath('.//meta[contains(@content, "?center=")][1]/@content').extract_first()
            if lat_lng:
                lat_lng = re.findall(r"-?[\d\.]+", lat_lng.split("?center=")[1].split("&")[0].replace("%2C", ""))
                item_loader.add_value('latitude', lat_lng[0])
                item_loader.add_value('longitude', lat_lng[1])

            map_query_postcode = maps_url.split("q=")[-1].split("&")[0]
            item_loader.add_value('zipcode', map_query_postcode)

        # address
        address = response.xpath('.//h1[contains(@class,"hs-line-11")]/text()').extract_first()
        item_loader.add_value('address', address)
        city = re.findall(r"\s[,a-zA-Z\s]+\s", " " + address + " ")
        city = [c_i.strip().strip(",") for c_i in city if "street" not in c_i.lower()]
        if len(city) > 0:
            item_loader.add_value('city', " ".join(city).split(",")[-1].strip())

        # ex https://www.hamiltons-sales.co.uk/notice.php?q=1006-imperial-house in key points
        if "Terrace" in features:
            item_loader.add_value('terrace', True)
        # ex https://www.hamiltons-sales.co.uk/notice?q=590-3-bedroom-apartment-for-rent in key features
        if 'Furnished' in features:
            item_loader.add_value('furnished', True)
        elif 'Unfurnished' in features:
            item_loader.add_value('furnished', False)
        # ex https://www.hamiltons-sales.co.uk/notice?q=590-3-bedroom-apartment-for-rent in key features
        if 'Elevator' in features:
            item_loader.add_value('elevator', True)
        # https://www.hamiltons-sales.co.uk/notice?q=1006-imperial-house
        if 'Washing Machine' in features:
            item_loader.add_value('washing_machine', True)
        # https://www.hamiltons-sales.co.uk/notice?q=993-property-for-sale
        if 'Dishwasher' in features:
            item_loader.add_value('dishwasher', True)
        # parking and balcony not in key points
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "HamiltonsSales_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
