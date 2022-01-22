# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re

import js2xml
import lxml
import scrapy
from geopy.geocoders import Nominatim
from scrapy import Selector

from ..helper import remove_white_spaces, extract_number_only, format_date
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class GibbsGillespieSpider(scrapy.Spider):
    name = 'gibbs_gillespie_co_uk'
    allowed_domains = ['gibbs-gillespie.co.uk']
    start_urls = ['https://www.gibbs-gillespie.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.gibbs-gillespie.co.uk/property-search/residential-houses-available-to-rent-in-south-east-england/page-1',
                'property_type': 'house',
                'param': 'houses'},
            {
                'url': 'https://www.gibbs-gillespie.co.uk/property-search/residential-bungalows-available-to-rent-in-south-east-england/page-1}',
                'property_type': 'house',
                'param': 'bungalows'},
            {
                'url': 'https://www.gibbs-gillespie.co.uk/property-search/residential-apartments-available-to-rent-in-south-east-england/page-1',
                'property_type': 'apartment',
                'param': 'apartments'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'param': url.get('param'),
                                       'property_type': url.get('property_type')})

    # def parse(self, response, **kwargs):
    #     no_of_properties = \
    #     response.xpath('.//span[@class="total-string"]/text()').extract_first().split("(")[1].split(")")[0]
    #     default_page_size = 12
    #     pages = int(int(no_of_properties) / default_page_size) + 1
    #     for page in range(1, pages + 1):
    #         req_url = f"https://www.gibbs-gillespie.co.uk/property-search/list-of-residential-{response.meta.get('param')}-available-to-rent-in-south-east-england/page-{page}"
    #         yield scrapy.Request(
    #             url=req_url,
    #             callback=self.get_property_links,
    #             meta={'property_type': response.meta.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="property-img"]/a/@href').extract()
        for property_item in listings:
            req_url = f"https://www.gibbs-gillespie.co.uk{property_item}"
            yield scrapy.Request(
                # manually called print url to get the bathroom as not present in main page
                url=req_url + "?layout=print",
                callback=self.get_bathrom,
                meta={'request_url': req_url,
                      'property_type': response.meta.get('property_type')})

        if len(listings) > 0:
            current_page = re.findall(r"(?<=page-)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page-)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                dont_filter=True,
                meta={'property_type': response.meta.get('property_type'),
                      'request_url': next_page_url})

    def get_bathrom(self, response):
        if response.xpath('.//li[@class="Bedrooms"]/text()'):
            bed = response.xpath('.//li[@class="Bedrooms"]/text()').extract_first()
        else:
            bed = response.xpath('.//li[@class="Bedroom"]/text()').extract_first()
        bathroom = response.xpath('.//li[@class="Bathroom"]/text()').extract_first()
        yield scrapy.Request(
            url=response.meta.get('request_url'),
            callback=self.get_property_details,
            meta={'request_url': response.meta.get('request_url'),
                  'property_type': response.meta.get('property_type'),
                  'bed': bed,
                  'bath': bathroom})

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')
        address = remove_white_spaces(response.xpath('.//div[@class="sales-title"]/address/text()').extract_first())
        available_date = response.xpath('.//span[contains(@class,"available-from")]/text()').extract_first().split(" ")[-1]
        rent_string = response.xpath('.//span[contains(@class,"house-price")]/span/text()').extract_first()
        landlord_phone = response.xpath('.//a[contains(@href,"tel")]/text()').extract_first()
        landlord_name = response.xpath('.//span[@class="agent-title"]/text()').extract_first()
        item_loader = ListingLoader(response=response)

        let_agreed = response.xpath("//span[@class='status-text']/text()").get()
        if let_agreed and 'let agreed' in let_agreed:
            return

        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split("/")[-1])
        item_loader.add_xpath('title', './/h1[@class="house-title"]/strong/text()')
        item_loader.add_value('address', address)
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('description', './/div[@class="timeline__fulltext"]/p/text()')
        item_loader.add_xpath('images', './/img[@class="stb_image-details-intro"]/@src')
        item_loader.add_value('room_count', extract_number_only(response.meta.get('bed')))
        item_loader.add_value('bathroom_count', extract_number_only(response.meta.get('bath')))
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_xpath('floor_plan_images', './/a[@class="view-floorplan"]/@href')

        javascript = response.xpath('.//script[contains(text(),"loadLocratingPlugin")]/text()').extract_first()
        # location = None
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//property[@name="lat"]/number/@value').extract_first()
            longitude = xml_selector.xpath('.//property[@name="lng"]/number/@value').extract_first()
        #     geolocator = Nominatim(user_agent=random_user_agent())
        #     location = geolocator.reverse(f"{latitude}, {longitude}")
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

        #     if location and 'address' in location.raw:
        #         if 'postcode' in location.raw['address']:
        #             item_loader.add_value('zipcode', location.raw['address']['postcode'])
        #         if 'city' in location.raw['address']:
        #             item_loader.add_value('city', location.raw['address']['city'])
        # if location is None:
        if len(item_loader.get_output_value('address').split(',')[-1].strip().split(' ')) == 2:
            item_loader.add_value('city', item_loader.get_output_value('address').split(',')[-2])
            item_loader.add_value('zipcode', item_loader.get_output_value('address').split(',')[-1].strip())
        elif len(item_loader.get_output_value('address').split(',')[-1].strip().split(' ')) > 2:
            item_loader.add_value('city', item_loader.get_output_value('address').split(',')[-1].strip().split(' ')[0])
            item_loader.add_value('zipcode', " ".join(item_loader.get_output_value('address').split(',')[-1].split(' ')[-2:]).strip())
        else:
            item_loader.add_value('city', item_loader.get_output_value('address').split(',')[-1])

        if available_date:
            item_loader.add_value('available_date', format_date(available_date))
        # ex https://www.gibbs-gillespie.co.uk/property-letting/flat-to-rent-in-tedder-close-uxbridge-middlesex-ub10-0fh/9029
        if 'parking' in item_loader.get_output_value('description').lower():
            item_loader.add_value('parking', True)

        # ex https://www.gibbs-gillespie.co.uk/property-letting/flat-to-rent-in-walbrook-court-amias-drive-edgware-ha8-8gx/9007
        if "balcony" in item_loader.get_output_value('description').lower():
            item_loader.add_value('balcony', True)
        # ex https://www.gibbs-gillespie.co.uk/property-letting/flat-to-rent-in-oak-end-way-gerrards-cross-sl9-8fn/1276
        if "terrace" in item_loader.get_output_value('description').lower():
            item_loader.add_value('terrace', True)

        # no furnished present
        # if " furnished" in item_loader.get_output_value('description'):
        #     item_loader.add_value('furnished', True)
        if " unfurnished" in item_loader.get_output_value('description').lower():
            item_loader.add_value('furnished', False)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "GibbsGillespie_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
