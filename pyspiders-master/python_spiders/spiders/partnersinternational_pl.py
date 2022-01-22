# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import math
class MySpider(Spider):
    name = 'partnersinternational_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="Partnersinternational_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://partnersinternational.pl/en/properties/apartments/rent",
                "property_type" : "apartment"
            },
            {
                "url" : "https://partnersinternational.pl/en/properties/houses/rent",
                "property_type" : "house"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='card-actions']/following-sibling::a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
          
        nextpage=response.xpath("//li[@class='pagination-next']/a/@href").get()  
        if nextpage:      
            yield Request(
                response.urljoin(nextpage),
                callback=self.parse,
                dont_filter=True,
                meta={"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        property_type=response.meta.get('property_type')
        if property_type:
            item_loader.add_value("property_type",property_type)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h2[@class='h4 text-color:gray listing-subtitle']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("city",title.split("|")[0].split(",")[-1].strip())
        rent=response.xpath("//p[@class='price-headline']/strong/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(" ",""))
        item_loader.add_value("currency","PLN")
        external_id=response.xpath("//p//text()[contains(.,'Offer n')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        room_count=response.xpath("//span[.='Rooms']/following-sibling::strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='Rooms']/following-sibling::strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//span[.='Area']/following-sibling::strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        description="".join(response.xpath("//div[@class='generic-content']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='title-media']//div//img//@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        balcony=response.xpath("//li[contains(.,'Balcony:')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        parking=response.xpath("//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        terrace=response.xpath("//li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        furnished=response.xpath("//li[contains(.,'Furnishings')]/text()").get()
        if furnished and "Yes" in furnished:
            item_loader.add_value("furnished",True)
        name=response.xpath("//h3[@class='card-headline']/a/text()").get()
        if name:
            item_loader.add_value("landlord_name",name.strip())
        email=response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email.split(":")[-1].strip())
        phone=response.xpath("//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1].strip())
        yield item_loader.load_item()