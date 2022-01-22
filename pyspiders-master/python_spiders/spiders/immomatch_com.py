# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'immomatch_com'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Immomatch_PySpider_belgium'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.immomatch.com/nl/te-huur?MinimumSoldPeriod=&SoldPeriod=&MinimumRentedPeriod=&RentedPeriod=365&FlowStatus=&ExcludeProjectRelatedItems=&EstateTypes=&OpenHouse=&Categories=&marketingtypes-excl=&reference-notlike=&sorts=Flat&transactiontype=Rent&sorts%5B%5D=Flat", "property_type": "apartment"},
	        {"url": "https://www.immomatch.com/nl/te-huur?MinimumSoldPeriod=&SoldPeriod=&MinimumRentedPeriod=&RentedPeriod=365&FlowStatus=&ExcludeProjectRelatedItems=&EstateTypes=&OpenHouse=&Categories=&marketingtypes-excl=&reference-notlike=&sorts=Dwelling&transactiontype=Rent&sorts%5B%5D=Dwelling", "property_type": "house"},

        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='card-text']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type",response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//h1[@class='wrap__heading ']/span/text()").get()
        if title:
            item_loader.add_value("title",title)
        address="".join(response.xpath("//div[@class='detail--header__container']//address//text()").getall())
        if address:
            item_loader.add_value("address",address.replace("\r","").replace("\n","").replace(" ",""))
        rent=response.xpath("//div[@class='detail--header__container']//h2//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace(" ","").replace("\xa0","").strip())
        item_loader.add_value("currency","EUR")
        description="".join(response.xpath("//div[@class='detail--text__container']/p/text()").getall())
        if description:
            item_loader.add_value("description",description)   
        room_count=response.xpath("//dt[.='Slaapkamers']/following-sibling::dd/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//dt[.='Badkamers']/following-sibling::dd/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//dt[.='Woonopp.']/following-sibling::dd/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        parking=response.xpath("//dt[.='Garage']/following-sibling::dd/text()").get()
        if parking and parking=="Ja":
            item_loader.add_value("parking",True)
        external_id=response.xpath("//td[.='Referentie:']/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        images=[x for x in response.xpath("//a[@class='detail-image']//@href").getall()]
        if images:
            item_loader.add_value("images",images)
        landlord_name=response.xpath("//h3[@class='card-title']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)
        phone=response.xpath("//div[@class='card-text']/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        item_loader.add_value("landlord_email","info@immomatch.com")

        yield item_loader.load_item() 

