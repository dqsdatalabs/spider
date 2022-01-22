# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found


class GroepnSpider(scrapy.Spider):
    name = "groepn"
    allowed_domains = ["www.groepn.be"]
    start_urls = (
        'http://www.www.groepn.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    external_source = "Groepn_PySpider_belgium_nl"
    thousand_separator='.'
    scale_separator=','


    def start_requests(self):
        start_urls = [
            {'url': 'https://www.groepn.be/te-huur/appartementen', 'property_type': 'apartment'},
            {'url': 'https://www.groepn.be/te-huur/woningen', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "property")]/a')
        for link in links:
            url = link.xpath('./@href').extract_first('')
            yield scrapy.Request(
                    url=url,
                    callback=self.get_property_details,
                    meta={'property_type': response.meta.get('property_type')},
                    dont_filter=True
            )
    def get_property_details(self, response): 
        if response.xpath('//h3[contains(text(), "Ligging")]/following-sibling::dl/div/dt[contains(text(), "Adres")]/following-sibling::dd/text()'): 
            address = response.xpath('//h3[contains(text(), "Ligging")]/following-sibling::dl/div/dt[contains(text(), "Adres")]/following-sibling::dd/text()').extract_first()
        elif response.xpath('//div[@class="property-sidebar"]//p[2]/text()'): 
            address = response.xpath('//div[@class="property-sidebar"]//p[2]/text()').extract_first()
        city_zipcode = address.split(',')[1]
        zipcode = re.findall(r'\d+', city_zipcode, re.S | re.M | re.I)[0]
        city = re.findall(r'([\w+\-]+)', city_zipcode)[1]
        property_type = response.meta.get('property_type')
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_id", response.url.split("/")[-1].strip())

        parking = response.xpath("//dt[contains(.,'Buitenparking')]/following-sibling::dd/text()").get()
        if parking: 
            if parking.lower().strip() == "ja": item_loader.add_value("parking", True)
            elif parking.lower().strip() == "nee": item_loader.add_value("parking", False)

        bathroom_count = response.xpath("//dt[contains(.,'Badkamers')]/following-sibling::dd[1]/text()").get()
        if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip())

        utilities = response.xpath("//dt[contains(.,'Gemeenschappelijke kosten')]/following-sibling::dd[1]/text()").get()
        if utilities: item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.split(",")[0].strip())))

        latitude_longitude = response.xpath("//script[contains(.,'lat =')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat =')[1].split(';')[0]
            longitude = latitude_longitude.split('lng =')[1].split(';')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_xpath("title", '//meta[@property="og:title"]/@content')
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[contains(@class, "property-description")]//text()')
        item_loader.add_xpath('rent_string', '//div[@class="price"]/text()')
        item_loader.add_xpath('images', '//div[@class="thumbnails"]/a/@href')
        item_loader.add_xpath('square_meters', '//span[contains(@class, "icon-area")]/following-sibling::p/text()')
        item_loader.add_xpath('room_count', '//span[contains(@class, "icon-rooms")]/following-sibling::p/text()')
        item_loader.add_value('landlord_name', 'N78 vastgoed')
        item_loader.add_value('landlord_email', 'info@n78vastgoed.be')
        item_loader.add_value('landlord_phone', '+32 89 86 18 88')
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()
