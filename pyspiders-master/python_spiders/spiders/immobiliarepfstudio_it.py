# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json

class MySpider(Spider):
    name = 'immobiliarepfstudio_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliarepfstudio_Pyspider_italy"

    # LEVEL 1
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.immobiliarepfstudio.it/risultati-ricerca-proprieta/?location=any&status=affitto&type=appartamento&bedrooms=any&min-price=any&max-price=any&min-area=&max-area=",
                ],
                "property_type": "apartment"
            },

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

        for item in response.xpath("//article[@class='property-item clearfix']//figure/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        rent=response.xpath("//h5//span[2]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(".")[0].replace("€","").strip())
        item_loader.add_value("currency","EUR")
        address=response.xpath("//h1[@class='page-title']/following-sibling::p/text()").get()
        if address:
            item_loader.add_value("address",address)
        city=item_loader.get_output_value("address")
        if city:
            item_loader.add_value("city",city.split("-")[-1])
        room_count=response.xpath("//span[contains(.,'Vani')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("\xa0")[0].replace("\n","").strip())
        bathroom_count=response.xpath("//span[contains(.,'Bagno') or contains(.,'Bagni')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("\xa0")[0].replace("\n","").strip())
        description=response.xpath("//div[@class='content clearfix']/p/text()").getall()
        if description:
            item_loader.add_value("description",description)
        parking=response.xpath("//span[contains(.,'Box')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        balcony=response.xpath("//h4[.='Caratteristiche']/following-sibling::ul//li/a/text()[.='Balconi']").get()
        if balcony:
            item_loader.add_value("balcony",True)
        latitude=response.xpath("//script[contains(.,'propertyMarkerInfo')]/text()").get()
        if latitude:
            latitude=latitude.split("propertyMarkerInfo")[1].split("icon")[0]
            item_loader.add_value("latitude",latitude.split("lat")[-1].split(",")[0].replace(":","").replace('"',""))
        longitude=response.xpath("//script[contains(.,'propertyMarkerInfo')]/text()").get()
        if longitude:
            longitude=longitude.split("propertyMarkerInfo")[1].split("icon")[0]
            item_loader.add_value("longitude",longitude.split("lang")[-1].split(",")[0].replace(":","").replace('"',""))
        square_meters=response.xpath("//div[@class='property-meta clearfix']//span[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        images=[x for x in response.xpath("//ul//li//a/@href[contains(.,'jpg')]").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_email"," info@immobiliarepfstudio.it")
        item_loader.add_value("landlord_name","Immobiliare PF studio")
        item_loader.add_value("landlord_phone","081 8916483")
        yield item_loader.load_item()