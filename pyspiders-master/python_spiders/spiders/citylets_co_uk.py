# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
import scrapy

class MySpider(Spider):
    name = 'citylets_co_uk'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    external_source = "Citylets_PySpider_united_kingdom"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    start_urls = [
        "https://www.citylets.co.uk/flats-rent-edinburgh/",
        "https://www.citylets.co.uk/flats-rent-aberdeen/",
        "https://www.citylets.co.uk/flats-rent-glasgow/",
        "https://www.citylets.co.uk/flats-rent-dundee/",
        "https://www.citylets.co.uk/flats-rent-renfrewshire/paisley/"
    ]
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, dont_filter=True)


    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div[@class='listing-img-wrapper']/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
  
        next_button =response.xpath("//a[@rel='next']/@href").get()
        if next_button:
            yield Request(response.urljoin(next_button), callback=self.parse)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type","apartment")

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres="".join(response.xpath("//h2[@itemprop='address']/span/text()").getall())
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//span[@itemprop='addressLocality']/text()").get()
        if city:
            item_loader.add_value("city",city)
        zipcode=response.xpath("//span[@itemprop='postalCode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        rent=response.xpath("//span[@class='rent']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("£")[-1].split("pcm")[0].replace(" ",""))
        item_loader.add_value("currency","GBP")
        room_count=response.xpath("//li[@class='property-beds']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//li[@class='property-bathroom']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        furnished=response.xpath("//li[.='Furnished']").get()
        if furnished:
            item_loader.add_value("furnished",True)
        furnished=response.xpath("//li[.='Unfurnished']").get()
        if furnished:
            item_loader.add_value("furnished",False)
        features=response.xpath("//ul[@class='property-details-list']/li/text()").getall()
        if features:
            for i in features:
                if "Parking" in i:
                    item_loader.add_value("parking",True)
                if "Garden" in i:
                    item_loader.add_value("terrace",True)
                if "Deposit" in i:
                    deposit=i.split("£")[-1]
                    if deposit:
                        item_loader.add_value("deposit",deposit)
        moredetails=response.xpath("//ul[@class='more-info-list']//li//text()").getall()
        if moredetails:
            for i in moredetails:
                if "Washing Machine" in i:
                    item_loader.add_value("washing_machine",True)
                if "No Pets Allowed" in i:
                    item_loader.add_value("pets_allowed",False)
        description="".join(response.xpath("//div[@class='read-more-content']//text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\r",""))
        images=[x for x in response.xpath("//img[contains(@alt,'Thumbnail')]/@data-lazy").extract()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","City Lets")


        
        
        yield item_loader.load_item()