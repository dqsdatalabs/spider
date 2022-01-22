# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
import re
import lxml
import js2xml
from scrapy import Selector
from ..helper import extract_number_only
import math

class AcorngroupCoUkSpider(scrapy.Spider):
    name = "acorngroup_co_uk"
    allowed_domains = ["www.acorngroup.co.uk"]
    start_urls = [
        {'url':'https://www.acorngroup.co.uk/property-search/houses-available-to-rent-in-london-and-kent/page-1',
        'property_type':'house'},
        {'url':'https://www.acorngroup.co.uk/property-search/flats-available-to-rent-in-london-and-kent/page-1',
        'property_type':'apartment'},
        {'url':'https://www.acorngroup.co.uk/property-search/bungalows-available-to-rent-in-london-and-kent/page-1',
        'property_type':'house'},
    ]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type':url.get('property_type'),
                    'request_url':url.get('url'),
                    'page':1})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"cards")]/div')
        for listing in listings:
            property_url = response.urljoin(listing.xpath('.//a/@href').extract_first())
            room_count = listing.xpath('.//i[contains(@class,"bedroom")]/following-sibling::span/text()').extract_first()
            if not room_count:
                room_count = listing.xpath('.//i[contains(@class,"living-room")]/following-sibling::span/text()').extract_first()
            bathroom_count = listing.xpath('.//i[contains(@class,"bathroom")]/following-sibling::span/text()').extract_first()            
            rent_string = listing.xpath('.//span[@class="price-qualifier"]/text()').extract_first()
            address = listing.xpath('.//p').extract_first()
            yield scrapy.Request(
                url=property_url, 
                callback=self.get_property_details, 
                meta={'request_url':property_url,
                    'property_type':response.meta.get('property_type'),
                    'bathroom_count':bathroom_count,
                    'room_count':room_count,
                    'rent_string':rent_string,
                    'address':address})

        if len(listings)==12:
            next_page_url = re.sub(r"page\-\d+", 'page-'+str(response.meta.get('page')+1), response.meta.get('request_url'))
            yield scrapy.Request(url=next_page_url,
                          callback=self.parse,
                          meta={'request_url': next_page_url,
                                'property_type':response.meta.get('property_type'),
                                'page':response.meta.get('page')+1})

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        external_id = re.findall(r"(?<=\/)\d+", response.meta.get('request_url'))
        if external_id:
            item_loader.add_value("external_id", external_id[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("room_count", response.meta.get('room_count'))
        item_loader.add_value("bathroom_count", response.meta.get('bathroom_count'))
        item_loader.add_value("rent_string", response.meta.get('rent_string'))

        address = response.meta.get('address')
        if address:
            item_loader.add_value("address", address)

            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode.split(" ")[0])

        title = response.xpath('.//title/text()').extract_first()
        if title:
            title = title.split(' | ')[0]
            item_loader.add_value("title", title)

        item_loader.add_xpath("description", './/div[@class="container"]/p/text()')

        item_loader.add_xpath("floor_plan_images", './/div[contains(@class,"floorplan-slider")]//img/@src')
        
        landlord_name = response.xpath('.//div[contains(@class,"contacts")]//span[contains(text(),"Email")]/text()').extract_first()
        if landlord_name:
            landlord_name = landlord_name.split('Email ')[-1]
            item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_xpath('landlord_phone', './/span[contains(text(),"Call")]/following-sibling::span/text()')

        javascript = response.xpath('.//script[contains(text(), "lng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//property[@name="lat"]/number/@value').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//property[@name="lng"]/number/@value').extract_first())

        images = response.xpath('.//style[contains(text(),"background-image")]/text()').extract_first()
        if images:
            images = re.findall(r'(?<=background-image:url\().+?(?=\))',images)
            item_loader.add_value("images", images)

        # https://www.acorngroup.co.uk/property-lettings/flat-to-rent-in-cable-walk-greenwich-se10/51561
        sqft = response.xpath('.//div[contains(@class,"list--links")]//a[contains(text(),"sq. ft.")]/text()').extract_first()
        if sqft:
            sqm = math.ceil(float(extract_number_only(sqft))*0.092903)
            item_loader.add_value('square_meters', str(sqm))

        # https://www.acorngroup.co.uk/property-lettings/flat-to-rent-in-stables-way-kennington-se11/50462
        energy_label = response.xpath('.//div[contains(@class,"list--links")]//a[contains(text(),\
            "Energy Efficiency Rating" or contains(text(),"EPC Rating:"))]/text()').extract_first()
        if energy_label:
            item_loader.add_value('energy_label', energy_label[-1])

        features = ' '.join(response.xpath('.//div[contains(@class,"list--links")]//a/text()').extract()).lower()

        # https://www.acorngroup.co.uk/property-lettings/flat-to-rent-in-mandela-street-stockwell-sw9/51169
        if re.findall(r'(?<!un)furnished', features):
            if not re.findall(r'(?<=un)furnished', features):
                item_loader.add_value('furnished', True)
        elif re.findall(r'(?<=un)furnished', features):
            item_loader.add_value('furnished', False)

        # https://www.acorngroup.co.uk/property-lettings/flat-to-rent-in-cable-walk-greenwich-se10/51561
        if re.findall(r'parking|garage', features):
            item_loader.add_value('parking', True)

        if re.findall(r'terrace', features):
            item_loader.add_value('parking', True)


        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
