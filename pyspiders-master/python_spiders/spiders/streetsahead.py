# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from scrapy import FormRequest
from ..loaders import ListingLoader
import re

class StreetsaheadSpider(scrapy.Spider):
    name = "streetsahead"
    allowed_domains = ["streetsahead.info"]
    start_urls = (
        'http://www.streetsahead.info/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Origin': 'https://www.streetsahead.info',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36',
        'Sec-Fetch-Dest': 'document',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'same-origin',
    }
    def start_requests(self):
        start_urls = 'https://www.streetsahead.info/renting/'
        data = {
            'ps-form-submission': 'true',
            'ps-form-reset-pages': 'true',
            'ps-page': '1',
            'ps-properties-per-page': 'all',
            'ps-sort-direction': 'price_desc',
            'ps-type': 'rent',
            'ps-area': 'all',
            'ps-min-price-buy': 'nomin',
            'ps-min-price-let': 'nomin',
            'ps-max-price-buy': 'nomax',
            'ps-max-price-let': 'nomax',
            'ps-min-beds': '2',
            'ps-max-beds': '2'
        }
        yield FormRequest(
            url=start_urls,
            formdata=data,
            headers=self.headers,
            dont_filter=True
        )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[contains(text(), "Full Details")]')
        for link in links: 
            url = link.xpath('./@href').extract_first()
            yield scrapy.Request(url=url, callback=self.get_property_details, headers=self.headers, dont_filter=True)
    def get_property_details(self, response):
        property_type = 'house'
        external_link = response.url
        address = ''.join(response.xpath('//h1[contains(@class, "propertyTitle")]/text()').extract()).strip()   
        room_count = '2'
        rent_string = ''.join(response.xpath('//h2[contains(@class, "price")]/text()').extract())
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        external_id = response.xpath("//link[contains(@rel,'shortlink')]//@href").get()
        if external_id:
            external_id = external_id.split("p=")[-1]
            item_loader.add_value('external_id', external_id)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        item_loader.add_value('address', address)
        
        desc = "".join(response.xpath('//section[@id="overview-page"]//p//text()').getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value('description', desc)
        
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//img[@class="property-gallery-image"]/@src')
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('landlord_name', 'Streets Ahead')
        item_loader.add_value('landlord_email', 'southnorwood@streetsahead.info')
        item_loader.add_value('landlord_phone', '020 8653 3333')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        parking = response.xpath("//li[contains(.,'PARKING')]").get()
        if parking:
            item_loader.add_value("parking", True)

        city = response.xpath("//h1/text()").get()
        if city: item_loader.add_value("city", city.split(",")[-1].strip())
        
        yield item_loader.load_item()
