# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
import json
from ..loaders import ListingLoader

class Studios2letSpider(scrapy.Spider):
    name = "studios2let"
    allowed_domains = ["www.studios2let.com"]
    start_urls = (
        'http://www.www.studios2let.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = 'https://www.studios2let.com/studio-flats-to-rent-in-london'
        data = {
            '_csrf': 'a2OYArJoZC-5M5d_dn-RJY05SWT14n2_TViTnKJfDf7dx8fb4ib431bg0hCgi2LTsgHWKuYwtK4A-BV6ExHVGw==',
            'location': '',
            'area': '',
            'tube': '',
            'search-submit': ''
        }
        yield scrapy.Request(
            url=start_urls,
            callback=self.parse, 
            body=json.dumps(data),
            dont_filter=True
        )

    def parse(self, response, **kwargs):
        # parse the detail url
        links = response.xpath('//div[@id="w0"]/div//a[@class="proptitle"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    
    def get_property_details(self, response):
        property_type_site = ''.join(response.xpath('//ul[contains(@class, "flat-page-header-list")]/li/text()').extract())
        if 'house' in property_type_site.lower():
            property_type = 'house'
        elif 'apartment' in property_type_site.lower():
            property_type = 'apartment'
        elif 'flat' in property_type_site.lower():
            property_type = 'apartment' 
        else:
            property_type = ''
        external_link = response.url
        address = response.xpath('//ul[contains(@class, "flat-page-header-list")]/li[2]/text()').extract_first('').strip()
        zipcode = address.split(',')[-1].split(' ')[-1]
        rent_string = response.xpath('//span[contains(text(), "1 month")]/following-sibling::span/text()').extract_first('').strip().split(' / ')[-1]
        latitude = re.search(r'lat:\s(.*?)\,', response.text).group(1)
        longitude = re.search(r'lng:\s(.*?)\}', response.text).group(1)
        if rent_string: 
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_xpath('title', '//h1[@class="sdn"]/text()')
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_xpath('description', '//h3[contains(text(), "Description")]/..//text()')
            item_loader.add_value('city', 'london')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//div[@id="links"]//img/@src')
            item_loader.add_value('latitude', str(latitude))
            item_loader.add_value('longitude', str(longitude))
            item_loader.add_value('landlord_name', 'Studios 2 Let')
            item_loader.add_value('landlord_email', 'flats@studios2let.com')
            item_loader.add_value('landlord_phone', '+44 (0) 20 7486 9020')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()