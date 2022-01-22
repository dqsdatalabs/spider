# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider 
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser
 
class MySpider(Spider):
    name = 'niya_nl_rotterdam'
    execution_type='testing'
    country='netherlands'
    locale='en'
    external_source="Niya_nl_rotterdam_PySpider_netherlands_en"
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://niya.nl/properties-search/?type=room&keyword&location=rotterdam&child-location=any&status=any&min-price=any&max-price=any#038;keyword&location=rotterdam&child-location=any&status=any&min-price=any&max-price=any"],
                "property_type" : "apartment"
            },      
            {
                "url" : ["https://niya.nl/properties-search/?type=studio&keyword=&location=rotterdam&child-location=any&status=any&min-price=any&max-price=any"],
                "property_type" : "studio"
            },            
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        
        for item in response.xpath("//div[@class='property_link']/@onclick").getall():
            follow_url = response.urljoin(item.split("='")[-1].split("';")[0])
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link",response.url)
        property_type = response.meta.get('property_type')
        prop_type = response.xpath("//main//span[contains(.,'Type')]/following-sibling::span/text()").get()
        if prop_type:
            if "Room" in prop_type:
                property_type = "room"
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        dontallow=response.xpath("//ol[@class='breadcrumb']//li[@class='active']//text()").get()
        if dontallow and "rented out" in dontallow.lower():
            return 

        item_loader.add_xpath("title", "//h1[contains(@class,'entry-title')]/text()")

        bathroom_count = response.xpath("//main//span[contains(.,'Bathroom')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        latitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('"lat":"')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latitude.split('"lang":"')[1].split('"')[0].strip())

        price = "".join(response.xpath("//span[contains(@class,'single-property-price')]/text()").extract())
        if price:
            item_loader.add_value("rent_string", price)

        images = [response.urljoin(x)for x in response.xpath("//ul/li/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        available_date = response.xpath(
            "normalize-space(//div//div[@class='date-output'][1]/text())").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        meters = "".join(response.xpath("normalize-space(//div[span[.='Size']]/span[@class='meta-item-value']/text())").extract())
        if meters:
            item_loader.add_value("square_meters", meters.strip())
            
        room_type = response.xpath("//div[@class='single-property-wrapper']//div[contains(@class,'meta-property-type')]//span[2]//text()").extract_first()
        if "Studio" in room_type or "Room" in room_type:
            item_loader.add_value("room_count","1")

        external_id = "".join(response.xpath("normalize-space(//div[span[.='Property ID']]/span[@class='meta-item-value']/text())").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = "".join(response.xpath("//ol/li/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())

        desc = "".join(response.xpath("//div[@class='property-content']/p/text()").extract())
        item_loader.add_value("description", desc.strip())

        balcony = response.xpath("//li/a[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("normalize-space(//div[span[.='Furnished']]/span[@class='meta-item-value']/text())").get()
        if "no" not in furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_phone", "+31 (0)10 413 56 77")
        item_loader.add_value("landlord_email", "info@niya.nl")
        item_loader.add_value("landlord_name", "Niye Rotterdam")


         
        yield item_loader.load_item()

        
       

        
        
          

        

      
     