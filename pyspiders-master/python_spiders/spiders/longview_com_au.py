# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
class MySpider(Spider):
    name = 'longview_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://longview.com.au/property-search/?property_type=22&minimum_bedrooms=&maximum_bedrooms=&minimum_rent=&maximum_rent=&address_keyword=&department=residential-lettings",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://longview.com.au/property-search/?property_type=9&minimum_bedrooms=&maximum_bedrooms=&minimum_rent=&maximum_rent=&address_keyword=&department=residential-lettings",
                    "https://longview.com.au/property-search/?property_type=88&minimum_bedrooms=&maximum_bedrooms=&minimum_rent=&maximum_rent=&address_keyword=&department=residential-lettings",
                    "https://longview.com.au/property-search/?property_type=86&minimum_bedrooms=&maximum_bedrooms=&minimum_rent=&maximum_rent=&address_keyword=&department=residential-lettings",
                    "https://longview.com.au/property-search/?property_type=87&minimum_bedrooms=&maximum_bedrooms=&minimum_rent=&maximum_rent=&address_keyword=&department=residential-lettings"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='details']/h3"):
            url = item.xpath("./a/@href").get()
            address = item.xpath("./a/text()").get()
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"address":address})
        
        next_page_url = response.xpath("//li/a[@class='next page-numbers']/@href").get()   
        if next_page_url:
            yield Request(next_page_url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Longview_PySpider_australia")  
        item_loader.add_xpath("title", "//h1[@class='property_title entry-title']/text()")     
        item_loader.add_xpath("external_id", "//li[@class='reference-number']/text()")  
        item_loader.add_xpath("room_count", "//li[@class='bedrooms']/text()") 
        item_loader.add_xpath("bathroom_count", "//li[@class='bathrooms']/text()") 
        item_loader.add_xpath("deposit", "//li[@class='deposit']/text()") 
        address = response.meta.get('address')
        if address:
            item_loader.add_value("address", address.strip())  
            item_loader.add_value("zipcode", "".join(address.strip().split(",")[-2:]))  
            item_loader.add_value("city", address.split(",")[-3].strip())  

        rent = response.xpath("//div[@class='price']/text()").get()
        if rent:
            price = rent.split("$")[-1].split("pw")[0].replace(",","")
            item_loader.add_value("rent", str(int(price)*4))
            item_loader.add_value("currency", "AUD")
        

        description = " ".join(response.xpath("//div[@class='description-contents']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//ul[@class='slides']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Longview")
        item_loader.add_value("landlord_phone", "1800 931 784")
     
        balcony = response.xpath("//div[@class='description-contents']//text()[contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)  

        parking = response.xpath("//div[@class='description-contents']//text()[contains(.,'car') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 
    

        yield item_loader.load_item()

