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
    name = 'morrison_watts_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Morrison_Watts_Co_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://watts-co.co.uk/search-results/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=&minimum_floor_area=&maximum_floor_area=&commercial_property_type=",
                ],
                "property_type":"house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("///a[.='More Details']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[.='→']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            ) 
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)      
        item_loader.add_value("property_type",response.meta.get("property_type"))
            
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")
        address = response.xpath("//h1[@class='property_title entry-title']/text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
        rent = response.xpath("//div[@class='price']/text()").get()
        if rent:
            price = rent.replace("£","").replace("pcm","").strip()
            item_loader.add_value("rent",price)
        item_loader.add_value("currency","GBP")
        external_id = response.xpath("//li[@class='reference-number']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())
        room_count = response.xpath("//li[@class='bedrooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count =response.xpath("//li[@class='bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        description ="".join(response.xpath("//div[@class='summary-contents']//p/text()").getall())
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x for x in response.xpath("//ul/li//img//@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)
 
        item_loader.add_value("landlord_phone", "0113 278 5555")
        item_loader.add_value("landlord_email", "info@watts-co.co.uk")
        item_loader.add_value("landlord_name", "Watts & Co")
        status = response.xpath("//h1[contains(.,'Parking')]//text()").get()
        yield item_loader.load_item()


