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
    name = 'primepropertypoland_com'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="Primepropertypoland_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://primepropertypoland.com/status/for-rent/",
                "property_type" : "apartment"
            },           
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='hover-effect']/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        nextpage=response.xpath("//a[@rel='Next']/@href").get()
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
        title=response.xpath("//div[@class='table-cell']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//span[@class='item-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("PLN")[0].replace(",","").strip())
        item_loader.add_value("currency","PLN")
        adres=response.xpath("//strong[.='Address:']/following-sibling::text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//strong[.='City:']/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city.strip())
        zipcode=response.xpath("//strong[.='Zip/Postal Code:']/following-sibling::text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        square_meters=response.xpath("//strong[.='Property Size:']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip().split(".")[0])
        room_count=response.xpath("//strong[.='Bedrooms:']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//strong[.='Bathrooms:']/following-sibling::text()").get()
        if bathroom_count and not bathroom_count.isalpha():
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//strong[.='Garage:']/following-sibling::text()").get()
        if parking:
            item_loader.add_value("parking",True)
        balcony=response.xpath("//a[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        dishwasher=response.xpath("//a[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
        washing_machine=response.xpath("//a[contains(.,'Washing machine')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        elevator=response.xpath("//a[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("property_lat")[-1].split(",")[0].replace('"',"").replace(":",""))
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("property_lng")[-1].split(",")[0].replace('"',"").replace(":",""))
        description="".join(response.xpath("//div[@id='description']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='item']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        name=response.xpath("//div[@class='media-body']/dl/dd/text()").get()
        if name:
            item_loader.add_value("landlord_name",name.strip())

        phone=response.xpath("//i[@class='fa fa-phone']/following-sibling::span/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1].strip())
        yield item_loader.load_item()