# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'michaelcharles_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.michaelcharles.co.uk/properties/lettings/tag-flat",
                ],
                "property_type" : "apartment"
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
        for item in response.xpath("//div[@id='togglable_list' ]//div[@class='photo_container']/../../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.michaelcharles.co.uk/properties/lettings/tag-flat/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta.get("property_type"), "page":page+1}
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//img[@class='property-status']/@src").get()
        if status and "let-agreed" in status:
            return
        item_loader.add_value("external_source", "Michaelcharles_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_css("title", "title")
        item_loader.add_value("external_link", response.url)

        address = response.xpath("//div[contains(@class,'prop')]/h2/text()").get()
        zipcode = address.strip().split(" ")[-1]
        count = address.count(",")
        
        if address:
            item_loader.add_value("address", address.strip())
        
        if count==1:
            item_loader.add_value("city",address.split(zipcode)[0].split(",")[0].strip())
        else:
            item_loader.add_value("city",address.split(zipcode)[0].split(",")[1].strip())
            
        zipcode1 = [zipcode if x.isdigit()else False for x in zipcode]
        for i in zipcode1:
            if i:
                item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[contains(@class,'prop')]/h2/span/text()").get()
        if rent and "pw" in rent:
            price = rent.split("pw")[0].split("£")[1].strip()
            item_loader.add_value("rent", str(int(price)*4))
        elif rent:
            price = rent.split("pcm")[0].split("£")[1].strip()
            item_loader.add_value("rent", price)
            
        item_loader.add_value("currency", "GBP")        
            
        desc = "".join(response.xpath("//div[@id='propertyDetails']//*[self::p][not(contains(.,'available'))]//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        room_count = response.xpath("//div[contains(@class,'Detail')]/ul/li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bed")[0].strip())
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//div[contains(@class,'Detail')]/ul/li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bath")[0].strip())
        
        available_date = "".join(response.xpath("//p/strong[contains(.,'available')]/parent::p/text()").getall())
        if available_date:
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("LatLng(")[1].split(",")[0]
            lng = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        floor_plan_images = response.xpath("//li/a/@title[contains(.,'Floor')]/parent::a/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        elevator = response.xpath("//ul[@id='points']/li[contains(.,'Lift') or contains(.,'lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//ul[@id='points']/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//ul[@id='points']/li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        unfurnished = response.xpath("//ul[@id='points']/li[contains(.,'Unfurnished') or contains(.,'unfurnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        elif unfurnished:
            item_loader.add_value("furnished", False)
            
        terrace = response.xpath("//ul[@id='points']/li[contains(.,'Terrace') or contains(.,'terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        
        item_loader.add_value("landlord_name", "MICHAEL CHARLES")
        phone = response.xpath("//div[@class='mobile-hide']/p/strong/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        
        #yield item_loader.load_item()
