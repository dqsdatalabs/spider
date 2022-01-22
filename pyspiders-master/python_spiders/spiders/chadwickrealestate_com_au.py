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

class MySpider(Spider):
    name = 'chadwickrealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Chadwickrealestate_Com_PySpider_australia"
    custom_settings = {              
        "PROXY_AU_ON" : True,
        "CONCURRENT_REQUESTS": 2,        
        "COOKIES_ENABLED": False,        
        "RETRY_TIMES": 3,   

    }
    download_timeout = 120
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Unit&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Apartment&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Flat&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Townhouse&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=House&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=DuplexSemi-detached&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Villa&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Terrace&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://chadwickrealestate.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Studio&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@id,'post')]/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@rel='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.xpath("//span[@class='page-price-rent']/span/text()").extract_first()
        if "deposit" in rent.lower():
            return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])
        item_loader.add_xpath("title", "//title/text()")

        rent = "".join(response.xpath("//div[contains(@class,'entry-2-col')]/div/div/span/span/text()").extract())     
        if rent:      
            price =  rent.strip().replace("pw","").strip().split(" ")[0].split("$")[1].replace(",","").strip()
            item_loader.add_value("rent",int(float(price))*4)
        item_loader.add_value("currency","AUD")

        item_loader.add_xpath("room_count", "normalize-space(//span[@title='Bedrooms']/span/text())")
        item_loader.add_xpath("bathroom_count", "normalize-space(//span[@title='Bathrooms']/span/text())")

        address = " ".join(response.xpath("//div[@class='entry-header-wrapper']/div//h1//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            city = " ".join(response.xpath("//div[@class='entry-header-wrapper']/div//h1//span/text()").getall())
            item_loader.add_value("city",city.strip())

        desc =  " ".join(response.xpath("//div[@class='listing-copy']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [ x.split("url(")[1].split(")")[0].strip() for x in response.xpath("//div[@id='slick-center-carousel']/div//@style").getall()]
        if images:
            item_loader.add_value("images", images) 


        item_loader.add_xpath("latitude","substring-before(substring-after(//script[contains(.,'LatLng')]/text(),'LatLng('),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(substring-after(//script[contains(.,'LatLng')]/text(),'LatLng('),','),')')")

        parking = "".join(response.xpath("normalize-space(//span[@title='Parking Spaces']/span/text())").extract())      
        if parking:
            (item_loader.add_value("parking", True) if "0" not in parking else item_loader.add_value("parking", False))

        item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='agent-name']/a/text())")
        item_loader.add_xpath("landlord_email", "normalize-space(//div[@class='agent-email']/a/text())")
        item_loader.add_xpath("landlord_phone", "normalize-space(//div[@class='agent-mobile']/a/text())")
        
        yield item_loader.load_item()