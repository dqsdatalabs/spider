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
import re


class MySpider(Spider):
    name = 'citybreakapartments_com'
    execution_type='testing'
    country='Ireland'
    locale='en'
    external_source = "Citybreakapartments_PySpider_Ireland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.citybreakapartments.com/properties/",
                ],
                "property_type" : "apartment",
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='all-properties']/div"):
            follow_url = item.xpath(".//div[@class='property_image']/a/@href").get()
            bathroom_count = item.xpath(".//img[contains(@data-lazy-src,'bathroom-icon.png')]/parent::span/text()").get()
            room_count = item.xpath(".//img[contains(@data-lazy-src,'bedroom-icon.png')]/parent::span/text()").get()
            if room_count:
                room = re.search("\d",room_count)
                if room:
                    room_count = room[0]
                else:
                    room_count = "1"
            else:
              room_count = "1"

            prop_type = item.xpath(".//h2[@class='propertydescription']/span/a/text()").get()
            if prop_type:
                if "studio" in prop_type.lower():
                    prop_type = "studio"
                elif "room" in prop_type.lower():
                    prop_type = "room"
                elif "apartment" in prop_type.lower():
                    prop_type = "apartment"
                else:
                    prop_type = "house"
            

            yield Request(follow_url, callback=self.populate_item, meta={"property_type":prop_type,"room_count":room_count,"bathroom_count":bathroom_count})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("room_count",response.meta.get("room_count"))
        item_loader.add_value("bathroom_count",response.meta.get("bathroom_count"))

        desc = " ".join(response.xpath("//div[@class='fl-rich-text'][2]/p").getall())
        if desc:
            item_loader.add_value("description",desc)
        else:
            desc = " ".join(response.xpath("//h2[a[text()='About']]/parent::div/p").getall())
            if desc:
                item_loader.add_value("description",desc)
 
        images = response.xpath("//div[@class='swiper-slide']/@data-src").getall()
        if images:
            item_loader.add_value("images",images)

        position = response.xpath("//script[contains(text(),'latitude')]").get()
        if position:
            lat = re.search('latitude":"([\d.]+)',position).group(1)
            long = re.search('latitude":"([\d.]+)","longitude":"([\d.]+)',position).group(2)
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)
        
        parking = response.xpath("//a[contains(text(),'Parking')]").get()
        if parking:
            item_loader.add_value("parking",True)

        title = response.xpath("//h1[@class='property-title entry-title']/text()").get()
        if title:
            item_loader.add_value("title",title)

        address = response.xpath("//div[@id='details']/p/text()").get()
        if address:
            address = address.strip().strip("|").strip()
            item_loader.add_value("address",address)

        external_id = response.xpath("//span[contains(text(),'Property ID')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[-1]
            item_loader.add_value("external_id",external_id)

        item_loader.add_value("landlord_email","info@dublincityapartments.ie")
        item_loader.add_value("landlord_name","City Break Apartments")
        item_loader.add_value("landlord_phone","+353 89 458 9611")
        item_loader.add_value("city","Dublin")
        item_loader.add_value("external_source",self.external_source)
        

        yield item_loader.load_item()