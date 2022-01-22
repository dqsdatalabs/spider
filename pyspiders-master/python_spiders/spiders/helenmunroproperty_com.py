# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'helenmunroproperty_com'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://helenmunroproperty.com/?action=epl_search&post_type=rental&property_status=current&property_category=Unit",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://helenmunroproperty.com/?action=epl_search&post_type=rental&property_status=current&property_category=House",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//h3[@class='entry-title']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'Next Page')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={'property_type': response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Helenmunroproperty_PySpider_australia")
        title = " ".join(response.xpath("//h2//text()[normalize-space()]").getall())
        if title:
            item_loader.add_value("title", title.strip())
     
        item_loader.add_xpath("room_count","//span[@title='Bedrooms']/span/text()")     
        item_loader.add_xpath("bathroom_count","//span[@title='Bathrooms']/span/text()")
        rent = response.xpath("//span[@class='page-price-rent']/span/text()").get()
        if rent:
            rent = rent.split("$")[-1].split("p")[0].replace(",","").strip()
            item_loader.add_value("rent", str(int(float(rent)*4)))
        item_loader.add_value("currency", "AUD")
        
        address = " ".join(response.xpath("//h2//text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.strip())
    
        item_loader.add_xpath("zipcode", "concat(//h2//span[@class='item-state']/text(), ' ',//h2//span[@class='item-pcode']/text())")
        item_loader.add_xpath("city", "//h2//span[@class='item-suburb']/text()")
        parking = response.xpath("//span[@class='icon parking']/span/text()").get()
        if parking:
            if parking.strip() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//li[@class='balcony']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
  
        description = " ".join(response.xpath("//div[@class='entry-column-content']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        available_date = response.xpath("//div[@class='property-meta date-available']/text()[.!='Available from now']").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("from")[-1], date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='gallery-1']//dt/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_value("landlord_name", "Helen Munro Property")
        item_loader.add_value("landlord_phone", "07 4759 3900")
        item_loader.add_value("landlord_email", "info@helenmunroproperty.com")
        yield item_loader.load_item()