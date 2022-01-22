# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy, copy, urllib
from ..loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, remove_unicode_char, format_date, remove_white_spaces
import re
import lxml,js2xml
from parsel import Selector
from scrapy import Request,FormRequest

class StudentchoiceplymouthComSpider(scrapy.Spider):
    name = "studentchoiceplymouth_com"
    allowed_domains = ["studentchoiceplymouth.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Studentchoiceplymouth_PySpider_united_kingdom_en"
    def start_requests(self):
        start_urls = [
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Flat"],"property_type" : "apartment"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Apartment"],"property_type" : "apartment"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=End+of+Terrace"],"property_type" : "house"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Maisonette"],"property_type" : "house"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Ground+Flat"],"property_type" : "apartment"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Studio"],"property_type" : "studio"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Terraced"],"property_type" : "house"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Terraced"],"property_type" : "house"},
            {"url" :["https://www.studentchoiceplymouth.com/search/?showstc=on&showsold=on&maxprice=&property_type=Terraced+Bungalow"],"property_type" : "house"},
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    def parse(self, response, **kwargs):

        for property_url in response.xpath("//div[@class='col-sm-12 col-md-12']/a/@href").getall():
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,meta={'property_type': response.meta.get('property_type')})
        next_page = response.xpath("//ol[@class='pagination']//a[@class='next']//@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            

            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        item_loader.add_value("external_link",external_link)
        item_loader.add_value("external_source",self.external_source)
       
        property_type = response.meta.get('property_type')
        item_loader.add_value("property_type",property_type)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//title//text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//h1[@class='h2']/strong/following-sibling::br/following-sibling::text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-1].replace("\t","").strip())
        rent=response.xpath("//h1[@class='h2']/strong/span/following-sibling::text()").get()
        if rent:
            if "week" in rent:
                rent=rent.split("per")[0].split("£")[1].strip()
                if rent:
                    item_loader.add_value("rent",int(rent)*4)
                    item_loader.add_value("currency","GBP")
        description="".join(response.xpath("//div[@class='col-sm-12 col-md-8 details']//h2//following-sibling::text()").getall())
        if description:
            item_loader.add_value("description",description)
        
        features=response.xpath("//ul[@class='tick']//li//text()").getall()
        if features:
            for i in features:
                if "Washing Machine" in i :
                    item_loader.add_value("washing_machine",True)
                if "Dish Washer" in i :
                    item_loader.add_value("dishwasher",True)
                if "Furnished" in i :
                    item_loader.add_value("furnished",True)
        images=[x for x in response.xpath("//div[@class='col-sm-2 col-md-2 row-nopad']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//title//text()").get()
        if room_count:
            for i in room_count.split(","):
                if "bed" in i.lower():
                    item_loader.add_value("room_count",i.strip().split(" ")[0])

        
        item_loader.add_value('landlord_name', 'Student Choice Plymouth')
        item_loader.add_value('landlord_phone', '01752 262222')
        item_loader.add_value('landlord_email', 'office@studentchoiceplymouth.com')

        yield item_loader.load_item()