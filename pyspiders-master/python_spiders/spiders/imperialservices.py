# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, format_date 
import dateparser

class ImperialservicesSpider(scrapy.Spider):
    name = "imperialservices"
    allowed_domains = ["imperialservices.co.uk"]
    start_urls = (
        'http://www.imperialservices.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    external_source = "Imperialservices_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = [
            {'url': 'http://imperialservices.co.uk/search?utf8=%E2%9C%93&trans_type=2&type=Flat&area=&price=&min_price=&max_price=&bedrooms=&by_let_date_available=&order_by=', 'property_type': 'apartment'},
            {'url': 'http://imperialservices.co.uk/search?utf8=%E2%9C%93&trans_type=2&type=House+Share&area=&price=&min_price=&max_price=&bedrooms=&by_let_date_available=&order_by=', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "property")]//h3/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//li[@class="next"]/a/@href'):
            next_link = response.urljoin(response.xpath('//li[@class="next"]/a/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        # parse details of the pro
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        external_link = response.url
        address = response.xpath('//h1[@class="panel-title"]/text()').extract_first()
        if address:
            city = address.split(', ')[-2]
            zipcode = address.split(', ')[-1] 
        external_id = response.xpath("//p/strong[.='Reference:']/following-sibling::text()[1]").extract_first()
        if external_id:
            item_loader.add_value('external_id', external_id.strip())

        room_count_text = response.xpath('//strong[contains(text(), "Bedrooms")]/../text()').extract_first('').strip()
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]    
            except:
                room_count = ''
        else:
            room_count = ''
        bathrooms_text = response.xpath("//p[strong[.='Bathrooms']]/text()").extract_first()
        if bathrooms_text: 
            item_loader.add_value('bathroom_count', bathrooms_text.replace(":",""))
        furnished = response.xpath('//strong[contains(text(), "Furnished")]/../text()').extract_first('').strip()
        rent_string = response.xpath('//strong[contains(text(), "Price")]/../text()').extract_first('').strip()
        available_date_text = response.xpath('//strong[contains(text(), "Available")]/../text()').extract_first('').replace(':', '')
        if available_date_text: 
            available_date = format_date(remove_white_spaces(available_date_text), '%d %B %Y')
        else:
            available_date = ''
        
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_value('title', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//h3[contains(text(), "description")]/following-sibling::p//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[contains(@class, "item")]/img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        parking = response.xpath("//li/text()[contains(.,'Parking') or contains(.,'parking')]").extract_first()
        if parking:
            item_loader.add_value('parking', True)
        if furnished:
            item_loader.add_value('furnished', True)
        if available_date:
            date_parsed = dateparser.parse(available_date)
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        item_loader.add_value('landlord_name', 'Imperial Services')
        item_loader.add_value('landlord_email', 'tom@cmscardiff.com')
        item_loader.add_value('landlord_phone', '02920 303040')
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item() 
    
