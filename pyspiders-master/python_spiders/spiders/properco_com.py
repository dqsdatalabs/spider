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
    name = 'properco_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="Properco_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.properco.com/lista.html?rent=1&object=Mieszkanie&loc-txt=&province=&location=&quarter=&type=&phrase=&symbol=&price%5Bfrom%5D=&price%5Bto%5D=&area%5Bfrom%5D=&area%5Bto%5D=&year_of_construction%5Bfrom%5D=&year_of_construction%5Bto%5D=&rooms%5Bfrom%5D=&rooms%5Bto%5D=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.properco.com/lista.html?rent=1&object=Dom&loc-txt=&province=&location=&quarter=&type=&phrase=&symbol=&price%5Bfrom%5D=&price%5Bto%5D=&area%5Bfrom%5D=&area%5Bto%5D=&year_of_construction%5Bfrom%5D=&year_of_construction%5Bto%5D=&rooms%5Bfrom%5D=&rooms%5Bto%5D=",
                "property_type" : "house"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 1)
        seen = False
        border=response.xpath("//li[@class='next']/preceding-sibling::li/a/text()").get()
        for item in response.xpath("//a[.='Show details']/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        if page == 2 or seen:
            if border and page<int(border)+3:
                nextpage=f"https://www.properco.com/lista.html?object=Mieszkanie&rent=1&location=&sort=&page={page}" 
                yield Request(
                    response.urljoin(nextpage),
                    callback=self.parse,
                    dont_filter=True,
                    meta={"page":page+1,"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        property_type=response.meta.get('property_type')
        if property_type:
            item_loader.add_value("property_type",property_type)
        external_id=response.xpath("//div[.='Listing No']/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        adress=response.xpath("//div[.='Location']/following-sibling::div/text()").get()
        if adress:
            item_loader.add_value("address",adress)
        item_loader.add_value("city",adress.split(",")[0])
        
        title=response.xpath("//h1[@class='section-header ']/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//div[.='Price:']/following-sibling::div/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("PLN")[0].strip().replace(" ",""))
        item_loader.add_value("currency","PLN")
        square_meters=response.xpath("//div[.='Total area [m2]']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0])
        room_count=response.xpath("//div[.='Rooms']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[.='No of bathrooms']/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        furnished=response.xpath("//div[.='Furnished']/following-sibling::div/text()").get()
        if furnished and "unfurnished"==furnished:
            item_loader.add_value("furnished",False)
        if furnished and ("yes"==furnished or "furnished"==furnished):
            item_loader.add_value("furnished",True)
        images=[x for x in response.xpath("//ul//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        name=response.xpath("//div[@class='contact-person__details']/span/text()").get()
        if name:
            item_loader.add_value("landlord_name",name.strip())
        email=response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email.split(":")[-1].strip())
        phone=response.xpath("//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1].strip())
        yield item_loader.load_item()