# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
class MySpider(Spider):
    name = 'kingsagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'       
    def start_requests(self):
        start_urls = [
            {
                "url": "https://kingsagents.co.uk/properties/?keyword=&offers=56&types=85&city=&noo_property_bedrooms=&noo_property_bathrooms=&noo_property_garages=&min_price=0&max_price=169995", 
                "property_type": "apartment"
            },
            {
                "url": "https://kingsagents.co.uk/properties/?keyword=&offers=56&types=84&city=&noo_property_bedrooms=&noo_property_bathrooms=&noo_property_garages=&min_price=0&max_price=169995", 
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//h4[@class='item-title']/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Kingsagents_Co_PySpider_united_kingdom")
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        address = response.xpath("//div[label[.='Address']]/span/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", " ".join(address.split(",")[-2].split(" ")[:-2]).strip())
        item_loader.add_xpath("zipcode", "//div[label[.='Postcode']]/span/text()")
    
        item_loader.add_xpath("bathroom_count", "//div[label[.='Bathrooms']]/span//text()")
        item_loader.add_xpath("room_count", "//div[label[.='Bedrooms']]/span//text()")
   
        rent_string = " ".join(response.xpath("//div[label[.='Price']]/span//text()").getall())
        if rent_string:
            item_loader.add_value("rent_string", rent_string.strip())
        description = " ".join(response.xpath("//div[@class='noo-detail-content']//text()[normalize-space()]").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        latitude = response.xpath("//input[@id='latitude']/@value").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//input[@id='longitude']/@value").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
       
        images = [response.urljoin(x) for x in response.xpath("//p/img[contains(@class,'alignnone size-medium')]/@src").getall()]
        if images:
            item_loader.add_value("images", images)
   
        item_loader.add_value("landlord_name", "Kings Sales & Letting Agents")
        item_loader.add_value("landlord_phone", "01642 73 00 00")
        item_loader.add_value("landlord_email", "info@kingsagents.co.uk")
        yield item_loader.load_item()