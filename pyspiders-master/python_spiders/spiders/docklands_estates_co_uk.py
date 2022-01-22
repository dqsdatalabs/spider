# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy
import re
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only
from ..user_agents import random_user_agent
import js2xml
import math
import lxml.etree
from parsel import Selector


class DocklandEstatesSpider(scrapy.Spider):
    name = "docklands_estates_co_uk"
    allowed_domains = ['docklands-estates.co.uk']
    start_urls = ['http://www.docklands-estates.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_url = ["http://www.docklands-estates.co.uk/property-search~for=2"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[@class="results-link"]')
        for listing in listings:
            property_url = listing.xpath('.//@href').extract_first()
            property_type = listing.xpath('.//span[@class="results-area"]/following-sibling::text()[1]').extract_first()
            property_type = re.sub(r"^[^\w]*", "", property_type)
            property_type = re.sub(r"[^\w]*$", "", property_type)
            if property_type in ["Apartment", "Flat", "House", "Penthouse", "Room Only", "Town House"]:
                yield scrapy.Request(
                    url=response.urljoin(property_url),
                    callback=self.get_property_details,
                    meta={'request_url': response.urljoin(property_url),
                          'property_type': property_type})
        
        next_page_url = response.xpath('//a[contains(text(),"Next")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(url= "http://www.docklands-estates.co.uk" + next_page_url,
                                 callback=self.parse,
                                 meta={'request_url': "http://www.docklands-estates.co.uk" + next_page_url})

    def get_property_details(self, response):
        external_link = response.meta.get('request_url')
        address = response.xpath('//h2[@class="details-address1"]/text()').extract_first()
        description = "".join(response.xpath('.//p[@class="detail-synopsis"]/following-sibling::*//text()').extract())
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split('pid=')[-1])

        # property_type
        property_type = response.xpath('.//div[contains(text(),"Type")]/span/text()').extract_first()
        property_mapping = {"apartment": "apartment", "flat": "apartment",
                            "house": "house",
                            "penthouse": "apartment",
                            "room only": "room",
                            "town house": "house"}
        if property_type:
            property_type = property_type.lower()
            for key_i in property_mapping:
                property_type = property_type.replace(key_i, property_mapping[key_i])
            item_loader.add_value('property_type', property_type)

        item_loader.add_xpath('room_count', './/div[contains(text(),"bedroom")]/span/text()')
        item_loader.add_xpath('bathroom_count', './/div[contains(text(),"Bathroom")]/span/text()')

        # square_meters
        javascript = response.xpath('.//div[contains(text(),"Area")]//script/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            area_a, area_b = selector.xpath('.//binaryoperation//number/@value').extract()
            area_a, area_b = float(area_a), float(area_b)
            if area_a*area_b*0.092903 > 0:
                item_loader.add_value('square_meters', " ".join([str(math.ceil(area_a*area_b*0.092903)), "sq meters"]))
        item_loader.add_xpath('title', './/div[@id="details-title"]/h1/text()')
        item_loader.add_value('description', description)
        item_loader.add_value('address', address)
        item_loader.add_value('city', address.split(', ')[1])
        item_loader.add_value('zipcode', address.split(', ')[-1])
        item_loader.add_xpath('rent_string', './/h4[@class="detail-price"]/text()')
        item_loader.add_xpath('rent', './/h4[@class="detail-price"]/text()')
        item_loader.add_xpath('images', './/div[@id="galleria"]//img/@src')
        item_loader.add_value('landlord_phone', '020 7790 7070')
        item_loader.add_value('landlord_name', 'Docklands Estates')
        item_loader.add_value('landlord_email', 'mail@docklands-estates.com')

        # latitude longitude
        lat_long_link = response.xpath('.//div[@id="maps"]//iframe/@src').extract_first()
        if lat_long_link and len(re.findall(r"(?<=cbll=)[^&]*", lat_long_link)) > 0:
            lat_long_link = re.findall(r"(?<=cbll=)[^&]*", lat_long_link)[0]
            item_loader.add_value('latitude', lat_long_link.split(',')[0])
            item_loader.add_value('longitude', lat_long_link.split(',')[1])

        # http://www.docklands-estates.co.uk/property-search~action=detail,pid=867
        parking = response.xpath('.//div[contains(text(), "Parking")]').extract_first()
        if parking or "parking" in description.lower():
            item_loader.add_value('parking', True)

        # http://www.docklands-estates.co.uk/property-search~action=detail,pid=557
        balcony = response.xpath('.//div[contains(text(), "Balcony")]').extract_first()
        if balcony or "balcony" in description.lower():
            item_loader.add_value('balcony', True)

        # http://www.docklands-estates.co.uk/property-search~action=detail,pid=1036
        terrace = response.xpath('.//div[contains(text(), "Terrace")]').extract_first()
        if terrace or "terrace" in description.lower():
            item_loader.add_value('terrace', True)
        
        # http://www.docklands-estates.co.uk/property-search~action=detail,pid=1036
        swimming_pool = response.xpath('.//div[contains(text(), "Swimming Pool")]').extract_first()
        if swimming_pool or "pool" in description.lower():
            item_loader.add_value('swimming_pool', True)

        elevator = response.xpath('.//div[contains(text(), "Elevator")]').extract_first()
        if elevator or "elevator" in description.lower():
            item_loader.add_value('elevator', True)

        # http://www.docklands-estates.co.uk/property-search~action=detail,pid=789
        furnished = response.xpath('.//div[contains(text(), "Furnished")]').extract_first()
        if furnished or "furnished" in description.lower():
            item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "DocklandsEstates_PySpider_{}_{}".format(self.country, self.locale))
        return item_loader.load_item()
