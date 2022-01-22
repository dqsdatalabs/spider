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
    name = 'marriottlane_com_au_disabled' 
    execution_type='testing'
    country='australia'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'  
    custom_settings = {
        "PROXY_US_ON": True,
    
    
    }
    external_source="Marriottlane_Com_PySpider_australia"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://marriottlane.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Unit&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://marriottlane.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Apartment&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                ],
                "property_type" : "apartment",

            },
            {
                "url" : [
                    "https://marriottlane.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Townhouse&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://marriottlane.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=House&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://marriottlane.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=DuplexSemi-detached&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://marriottlane.com.au/?search_channel=&action=epl_search&post_type=rental&search_channel=leased&property_status=current&property_category=Studio&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
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

        for item in response.xpath("//div[@class='epl-post-container']/div[contains(@id,'post')]/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'›')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.url.split("-")[-1].replace("/",""))
        item_loader.add_xpath("title", "//title/text()")

        rent = response.xpath("//span[@class='bond']/text()[contains(.,'$')]").extract_first()
        if rent:
            price =  rent.split(" Bond")[0].replace("$","").strip()
            item_loader.add_value("rent",price)
        else:
            rent = response.xpath("//span[@class='page-price']/text()[contains(.,'$')]").extract_first()
            if rent:
                price =  rent.split("per")[0].replace("$","").strip()
                item_loader.add_value("rent",int(float(price))*4)


        item_loader.add_value("currency","AUD")

        deposit = response.xpath("//span[@class='bond']/text()").extract_first()
        if deposit: 
            deposit =  deposit.split(" ")[0].strip()
            item_loader.add_value("deposit",deposit)
        dontallow=response.xpath("//span[@class='page-price']/text()").get()
        if dontallow and "deposit taken" in dontallow.lower():
            return 


        address = " ".join(response.xpath("//div[@class='property-address test']/h2/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        item_loader.add_xpath("room_count", "//span[@title='Bedrooms']/span/text()")
        if "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count", "1")
        item_loader.add_xpath("bathroom_count", "//span[@title='Bathrooms']/span/text()")

        available_date=response.xpath("//div[@class='property-meta date-available']/text()").get()
        if available_date:
            date2 =  available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        item_loader.add_xpath("latitude","substring-before(substring-after(//script[contains(.,'LatLng')]/text(),'LatLng('),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(substring-after(//script[contains(.,'LatLng')]/text(),'LatLng('),','),')')")
        desc =  " ".join(response.xpath("//div[@class='listing-description']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [ x for x in response.xpath("//div[@class='slick-center-carousel']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        parking = response.xpath("//span[@title='Parking Spaces']/span/text()").extract_first()
        if parking  :
            (item_loader.add_value("parking", True) if "0" not in parking else item_loader.add_value("parking", False))

        item_loader.add_xpath("landlord_name", "//div[@class='agent-contact']/span/text()")
        item_loader.add_xpath("landlord_phone", "substring-after(//div[@class='agent-contact']/p[contains(.,'Mobile')]/text(),': ')")
        item_loader.add_value("landlord_email", "leasing@marriottlane.com.au") 

 



        yield item_loader.load_item()