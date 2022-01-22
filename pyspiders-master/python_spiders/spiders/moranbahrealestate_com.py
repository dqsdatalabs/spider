# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import re
import dateparser

class MySpider(Spider):
    name = 'moranbahrealestate_com'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "http://www.moranbahrealestate.net.au/?json/listing/restype/5,6/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/120/leaseddays/120",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.moranbahrealestate.net.au/?json/listing/restype/7,9,39,40,11,15/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/120/leaseddays/120",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://www.moranbahrealestate.net.au/?json/listing/restype/1/orderby/new-old/page/1/filterType/residentialRental/leased/false/solddays/120/leaseddays/120",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        selector = scrapy.Selector(text=data["BODY"], type="html")
        for item in selector.xpath("//div[contains(@id,'listing-')]/a/@href").getall():
            seen = True
            follow_url = "http://www.moranbahrealestate.net.au/" + item.replace("\\", "").replace('"', "")
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("page/" + str(page - 1), "page/" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Moranbahrealestate_PySpider_australia")   
        item_loader.add_xpath("title", "//title/text()")   
        external_id = response.xpath("//span[contains(.,'Property ID')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        address = " ".join(response.xpath("//meta[@name='Description']/@content").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address)) 
            zipcode = " ".join(address.split(" ")[-2:]) 
            item_loader.add_value("zipcode", zipcode) 
        item_loader.add_xpath("city", "//h1/strong/text()")
        room_count = response.xpath("//div[contains(@class,'filter-bed')]/text()[normalize-space()][not(contains(.,'0'))]").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        elif "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count","1")

        item_loader.add_xpath("bathroom_count", "//div[contains(@class,'filter-bath')]/text()[normalize-space()]")
       
        parking = response.xpath("//div[contains(@class,'filter-car')]/text()[normalize-space()]").get()
        if parking:
            if "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        furnished = response.xpath("//span[.='Furnished']//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        dishwasher = response.xpath("//span[.='Dishwasher']//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        balcony = response.xpath("//span[.='Balcony']//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        item_loader.add_value("currency", "AUD")
        rent = response.xpath("//h4//text()").get()
        if rent:
            rent = rent.split()[0].strip(" $").split(".")[0]
            item_loader.add_value("rent", int(rent)*4)
        deposit = response.xpath("//span[contains(.,'Bond')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("$")[-1])
        
        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//div[contains(@class,'property-dates')]//span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [x for x in response.xpath("//div[@class='webchoice-gallery-sidewrapper']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        map_coord = response.xpath("//div[@class='gmapDiv']/iframe/@src").get()
        if map_coord:
            item_loader.add_value("latitude", map_coord.split("&q=")[-1].split(",")[0].strip())
            item_loader.add_value("longitude", map_coord.split("&q=")[-1].split(",")[1].strip())
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'property-staff')]//h3/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'property-staff')]//a[contains(@href,'tel')]/span/text()")
        item_loader.add_value("landlord_email", "moranbahrealestate@bigpond.com")
        
        json_image_id = response.url.split("/property/")[1].split("/")[0].strip()
        json_image_url = f"http://www.moranbahrealestate.net.au/?/gallery/{json_image_id}"
        if json_image_url:
            yield Request(
                json_image_url,
                callback=self.get_image,
                meta={
                    "item_loader" : item_loader 
                }
            )
        else:
            yield item_loader.load_item()

    def get_image(self, response):
        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        images = [x["src"] for x in data]
        if images:
            item_loader.add_value("images", images)
        yield item_loader.load_item()