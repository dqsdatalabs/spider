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

class MySpider(Spider):
    name = 'kitsonresidential_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    external_source = "Kitsonresidential_PySpider_united_kingdom"
    
    custom_settings = {
        #"PROXY_ON": True,
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],    
        "HTTPCACHE_ENABLED": False
    }
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "accept-language": "en-US,en;q=0.9,tr;q=0.8"
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://kitsonresidential.com/property-search/page/1/?status=for-rent&type%5B%5D=residential&type%5B%5D=apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://kitsonresidential.com/property-search/page/1/?status=for-rent&type%5B%5D=detached-bungalow&type%5B%5D=end-terrace-house&type%5B%5D=mid-townhouse&type%5B%5D=mid-terrace-house&type%5B%5D=villa&type%5B%5D=semi-detached",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse, 
                            headers=self.headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@class='more-details']/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item,headers=self.headers, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen:
            follow_url = f"https://kitsonresidential.com/property-search/page/{page}/?" + response.url.split('?')[-1]
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//span[@class='status-label ']/text()[contains(.,'Let')]").get()
        if status:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)     

        item_loader.add_xpath("external_id", "substring-after(//link[@rel='shortlink']/@href,'p=')")
        
        item_loader.add_xpath("title","//h1/span/text()") 
        city = response.xpath("//div[@class='page-breadcrumbs ']//li[last()]/a/text()").get()
        if city:
            item_loader.add_value("city", city)
        address = " ".join(response.xpath("//div/address/text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address) 
            zipcode = address.replace(", United Kingdom","").split(",")[-1].replace("\u00a0"," ").strip()
            item_loader.add_value("zipcode"," ".join(zipcode.split(" ")[-2:]) ) 
        else:
            item_loader.add_xpath("address","//h1/span/text()") 
        available_date = response.xpath("//li[contains(.,'Available')]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[1].replace("Mid-","").strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        room_count = response.xpath("//span[@class='property-meta-bedrooms']//text()[normalize-space()]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bed")[0].strip())
       
        bathroom_count = response.xpath("//span[@class='property-meta-bath']//text()[normalize-space()]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bath")[0].strip())
        balcony = response.xpath("//li[.='Balcony']//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        terrace = response.xpath("//li[contains(.,'Terrace') or contains(.,'terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[a[.='Balcony']]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//div[@id='property-slider-two']//ul[@class='slides'][1]/li//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        else:
            images = [x for x in response.xpath("//div[@id='property-detail-flexslider']//ul[@class='slides'][1]/li//a/@href | //div[@id='property-featured-image']//a/@href").getall()]
            if images:
                item_loader.add_value("images", images)
        rent ="".join(response.xpath("//span[@class='price-and-type']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))
        script_map = response.xpath("//script[contains(.,'lng')]/text()").get()
        if script_map:
            lat = script_map.split('"lat":"')[1].split('"')[0].strip()
            lng = script_map.split('"lng":"')[1].split('"')[0].strip()
            if lat != "0":
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
        landlord_name = response.xpath("//div[contains(@class,'agent-detail')]/div[@class='left-box']/h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)
            item_loader.add_xpath("landlord_phone", "//div[contains(@class,'agent-detail')]//li[@class='mobile']/a/text()")
        else:
            item_loader.add_value("landlord_name", "Kitson Residential")
            item_loader.add_value("landlord_phone", "02890 388344")
        item_loader.add_value("landlord_email", "info@kitsonresidential.com")
     
        yield item_loader.load_item()