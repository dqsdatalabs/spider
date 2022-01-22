# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found

class Place4youSpider(scrapy.Spider):
    name = "place4you"
    allowed_domains = ["www.place4you.be"]
    start_urls = (
        'http://www.www.place4you.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        url = "https://www.place4you.be/en-GB/List/21" 
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "estate-thumb-container")]')
        for link in links:
            url = response.urljoin(link.xpath('./a/@href').extract_first(''))
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
        if response.xpath('//a[contains(text(), "next page")]/@href'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), "next page")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True)
    
    def get_property_details(self, response):
        external_id = response.url.split('/')[-1]
        title = response.xpath('//head/meta[@property="og:title"]/@content').extract_first().strip()
        if 'flat' in title.lower() or 'duplex' in title.lower():
            property_type = 'apartment'
        elif 'villa' in title.lower() or 'maison' in title.lower() or 'house' in title.lower():
            property_type = 'house'
        else:
            property_type = ''
        address = ''.join(response.xpath('//i[contains(@class, "map-marker")]/following-sibling::text()').extract()).strip()
        city_zip = address.split(' ')[-1]
        zipcode = re.findall(r'\d+', city_zip)[0]
        city = city_zip.replace(zipcode, '').strip()
        if property_type:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('title', '//meta[@property="og:title"]/@content')
            item_loader.add_xpath('description', '//head/meta[@property="og:description"]/@content')
            item_loader.add_xpath('floor', '//th[contains(text(), "Floor")]/following-sibling::td/text()')
            item_loader.add_xpath('images', '//img[@class="img-slider-main"]/@src')
            item_loader.add_xpath('square_meters', '//th[contains(text(), "Habitable surface")]/following-sibling::td/text()')
            item_loader.add_xpath('room_count', '//th[contains(text(), "bedroom")]/following-sibling::td/text()')
            item_loader.add_xpath('bathroom_count', '//th[contains(text(), "bathroom")]/following-sibling::td/text()')
            item_loader.add_value('landlord_name', 'Isabelle Sandbergen')
            item_loader.add_value('landlord_email', 'info@place4you.be')
            item_loader.add_value('landlord_phone', '0495 53 41 10')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            
            rent = response.xpath("//span[@class='estate-text-emphasis']/text()").get()
            if rent:
                rent = rent.split("€")[0].strip().replace(",","")
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", "EUR")
            
            deposit = response.xpath("//th[contains(text(), 'guarantee')]/following-sibling::td/text()").get()
            if deposit:
                deposit = int(deposit)*int(rent)
                item_loader.add_value("deposit", deposit)

            charges = response.xpath("//th[contains(text(), 'Charges')]/following-sibling::td/text()").get()
            if charges:
                item_loader.add_value("utilities", charges.strip())
            parking = response.xpath("//th[contains(text(), 'Parking')]/following-sibling::td/text()").get()
            if parking:
                if parking.strip().lower() == 'no':
                    item_loader.add_value("parking", False)
                else:
                    item_loader.add_value("parking", True)
            furnished = response.xpath("//th[contains(text(), 'Furnished')]/following-sibling::td/text()").get()
            if furnished:
                if furnished.strip().lower() == 'no':
                    item_loader.add_value("furnished", False)
                else:
                    item_loader.add_value("furnished", True)
            
            yield item_loader.load_item()
