# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class AmbercourtlettingsSpider(scrapy.Spider):
    name = "ambercourtlettings"
    allowed_domains = ["ambercourtlettings.co.uk"]
    start_urls = (
        'http://www.ambercourtlettings.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'http://ambercourtlettings.co.uk/property-to-let', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[contains(text(), "Read more")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//ul[contains(@class, "pagination-list")]//a[contains(text(), "Next")]/@href'):
            next_link = response.urljoin(response.xpath('//ul[contains(@class, "pagination-list")]//a[contains(text(), "Next")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            
    def get_property_details(self, response):
        # parse details of the property
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//b[contains(text(), "Ref")]/following-sibling::text()').extract_first().strip().split(': ')[-1]
        address = ''.join(response.xpath('//property[@class="item-page"]//h1/text()').extract())
        room_count = response.xpath('//img[contains(@src, "bedrooms")]/following-sibling::strong/text()').extract_first('').strip()
        bathrooms = response.xpath('//img[contains(@src, "bathrooms")]/following-sibling::strong/text()').extract_first('').strip()
        try:
            lat = re.search(r'lat\:\s\"(.*?)\"', response.text).group(1)
            lon = re.search(r'lat\:\s\"(.*?)\"', response.text).group(1)
        except:
            lat = ""
            lon = ""
        
        rent_string = response.xpath('//small[@class="eapow-detail-price"]/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        
        status = response.xpath('//div[@class="eapow-bannertopright"]/img/@alt').get()
        if status == 'Let STC':
            return
        
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)

        city = response.xpath("//h1/text()").get()
        if city: item_loader.add_value('city', city.split(",")[-1].strip())
        item_loader.add_xpath('title', '//h1/text()')
        
        item_loader.add_xpath('description', '//div[@id="propdescription"]//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[@id="slider"]//img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lon))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        
        furnished = response.xpath("//li[contains(.,'FURNISHED') or contains(.,'Furnished') or contains(.,' furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//li[contains(.,'PARKING') or contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        zipcode = response.xpath("//address/text()").get()
        if zipcode:
            zipcode = zipcode.split(" ")
            item_loader.add_value("zipcode", f"{zipcode[-2]} {zipcode[-1]}")
        
        deposit = response.xpath("//div[@id='propdescription']//text()[contains(.,'Deposit (')]").get()
        if deposit:
            deposit = deposit.split("week")[0].strip().split(" ")[-1]
            try:
                from word2number import w2n
                deposit = w2n.word_to_num(deposit)
                rent = rent_string.split("£")[-1].strip()
                item_loader.add_value("deposit", int(float(rent))*int(deposit))
            except: pass
        
        item_loader.add_value('landlord_name', 'Amber Court')
        item_loader.add_value('landlord_email', 'info@ambercourtlettings.co.uk')
        item_loader.add_value('landlord_phone', '+44 (0)121 689 8080')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        yield item_loader.load_item()