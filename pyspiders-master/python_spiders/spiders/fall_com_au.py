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
    name = 'fall_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Fall_Com_PySpider_australia'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.fall.com.au/?action=epl_search&post_type=rental&property_category=Unit",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.fall.com.au/?action=epl_search&post_type=rental&property_category=House",
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
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])
        item_loader.add_value("external_source", self.external_source)          
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("room_count", "(//div[@class='property-feature-icons epl-clearfix']//div[@class='epl-icon-svg-container epl-icon-container-bed']//text())[3]")
        item_loader.add_xpath("bathroom_count", "(//div[@class='property-feature-icons epl-clearfix']//div[@class='epl-icon-svg-container epl-icon-container-bath']//text())[3]")
        rent = "".join(response.xpath("//span[@class='page-price-rent']//text()").getall())
        if rent:
            rent = rent.split("$")[1].lower().split('p')[0].split("/")[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
 
        address = response.xpath("//span[@class='entry-title-sub']//span//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//span[@class='entry-title-sub']//span[@class='item-state']//text()").get()
        if city:
            item_loader.add_value("city", city.strip())
           
        zipcode = response.xpath("//span[@class='entry-title-sub']//span[@class='item-pcode']//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        description = " ".join(response.xpath("//div[contains(@class,'rich-text')]//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        

        images = [x for x in response.xpath("//img[@loading='lazy']//@src").extract()]
        if images :
            item_loader.add_value("images", images)  

        if response.xpath("//div[@id='agent']//h4/text()").get():
            item_loader.add_xpath("landlord_name", "//div[@id='agent']//h4/text()")
        else: item_loader.add_value("landlord_name", "Fall Real Estate")
        
        if response.xpath("//div[contains(@class,'property-agents')]//div[@id='agent']//a[i[@class='fa fa-mobile']]/div/text()").get():
            item_loader.add_xpath("landlord_phone", "//div[contains(@class,'property-agents')]//div[@id='agent']//a[i[@class='fa fa-mobile']]/div/text()")
        else: item_loader.add_value("landlord_phone", "03 6234 7033")
            
        if response.xpath("//div[contains(@class,'property-agents')]//div[@id='agent']//a[i[@class='fa fa-envelope']]/div/text()").get():
            item_loader.add_xpath("landlord_email", "//div[contains(@class,'property-agents')]//div[@id='agent']//a[i[@class='fa fa-envelope']]/div/text()")
        else: item_loader.add_value("landlord_email", "realestate@fall.com.au")

        yield item_loader.load_item()