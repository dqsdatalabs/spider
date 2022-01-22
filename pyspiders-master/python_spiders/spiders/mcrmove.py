# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import format_date 
import re

class McrmoveSpider(scrapy.Spider):
    name = "mcrmove"
    allowed_domains = ["www.mcrmove.co.uk"]
    start_urls = (
        'http://www.www.mcrmove.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {
                'url': {
                    "https://www.mcrmove.co.uk/search/?instruction_type=Letting&address_keyword=",
                },
                'property_type': 'house'
            }

        ]
        for url in start_urls:
            for item in url.get('url'):
                yield scrapy.Request(
                    url=item,
                    callback=self.parse, 
                    meta={'property_type': url.get('property_type')},
                    dont_filter=True
                )

    def parse(self, response, **kwargs):
        
        page = response.meta.get('page', 2)
        seen = False
        links = response.xpath('//a[contains(text(), "Details")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        # if page==2 or seen:
        #     f_url = response.url.replace(f"{page-1}.html", f"{page}.html")
        #     yield scrapy.Request(f_url, callback=self.parse, meta={"property_type": response.meta.get('property_type'), "page":page+1})
            
    def get_property_details(self, response):
        external_link = response.url
        property_type = response.meta.get('property_type')
        rented = response.xpath("//span[@itemprop='availability']/text()[contains(.,'Let Agreed')]").extract_first()
        if rented:
            return
        address = response.xpath('//span[@itemprop="name"]/text()').extract_first('').strip()
        city = address.split(', ')[-1]   
        external_id = response.xpath('//li//p[contains(text(), "ref")]/text()').extract_first('').strip().split(' - ')[-1]
        room_count = response.xpath('//li[@class="bedrooms"]/text()').extract_first('').strip()
        bathrooms = response.xpath('//li[@class="bathrooms"]/text()').extract_first('').strip() 
        rent_string = response.xpath('//span[@itemprop="price"]/@content').extract_first('').strip() + '£'
        item_loader = ListingLoader(response=response)
        
        import dateparser
        available_date_text = response.xpath('//li[contains(text(), "Available")]/text()').extract_first('').split(' ')[-1]
        if available_date_text:
            date_parsed = dateparser.parse(available_date_text, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', str(external_id))

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        
        desc = " ".join(response.xpath('//div[contains(@class, "property-description")]/p//text()').getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//img[@itemprop="image"]/@src')
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('bathroom_count', bathrooms)
        
        deposit = response.xpath("//li[contains(.,'£')]/text()").get()
        if deposit:
            deposit = deposit.split("£")[1].split("De")[0].split("p")[0].strip()
            item_loader.add_value("deposit", int(float(deposit)))
        
        external_id = response.xpath("//li[contains(.,'Ref')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("-")[1].strip())
        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value('landlord_name', 'MCR MOVE')
        item_loader.add_value('landlord_email', ' info@mcrmove.co.uk')
        item_loader.add_value('landlord_phone', ' 0161 2486277')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        yield item_loader.load_item() 