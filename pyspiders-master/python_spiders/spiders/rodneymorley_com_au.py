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
    name = 'rodneymorley_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=Apartment&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=Unit&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=Flat&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=Townhouse&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=House&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=DuplexSemi-detached&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=Villa&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=Terrace&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://rodneymorley.com.au/?action=epl_search&post_type=rental&property_status=current&property_location=&property_category=Studio&property_price_from=&property_price_to=&property_bedrooms_min=&property_bedrooms_max=&property_bathrooms=",
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

        for item in response.xpath("//div[@class='viewprop']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        item_loader.add_value("external_source", "Rodneymorley_Com_PySpider_australia")
        item_loader.add_xpath("title", "//title/text()")

        address = "".join(response.xpath("//div[@class='property-details']/h1//text()").extract())
        if address:     
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        item_loader.add_xpath("city", "//div[@class='property-details']/h1/span/span[@class='item-suburb']/text()")
        item_loader.add_xpath("zipcode", "//div[@class='property-details']/h1/span/span[@class='item-state']/text()| //div[@class='property-details']/h1/span/span[@class='item-pcode']/text()")

        rent = response.xpath("//span[@class='page-price-rent']/span/text()").extract_first()
        if rent:
            price = rent.split(" ")[0].replace("\xa0",".").replace(",",".").replace(" ","").replace("$","").strip().replace("per","").replace("pw","").strip()
            if price !="NC":
                item_loader.add_value("rent", int(float(price))*4)
                item_loader.add_value("currency", "AUD")

        if response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_xpath("room_count", "normalize-space(//span[@title='Bedrooms']/span/text())")

        item_loader.add_xpath("bathroom_count", "normalize-space(//span[@title='Bathrooms']/span/text())")
        if response.xpath("//div[@class='epl-tab-section']//@data-cord").get():
            item_loader.add_xpath("latitude", "substring-before(//div[@class='epl-tab-section']//@data-cord,',')")
        else:
            latitude = response.xpath("//div/@data-cord").get()
            if latitude:
                item_loader.add_value("latitude", latitude.split(",")[0].strip())

        if response.xpath("//div[@class='epl-tab-section']//@data-cord"):
            item_loader.add_xpath("longitude", "substring-after(//div[@class='epl-tab-section']//@data-cord,', ')")
        else:
            latitude = response.xpath("//div/@data-cord").get()
            if latitude:
                item_loader.add_value("longitude", latitude.split(",")[-1].strip())

        available_date=response.xpath("normalize-space(//div[@class='tab-content']/div[@class='date-available']/text())").get()
        if available_date:
            date2 =  available_date.split(" ")[-1].strip()
            item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//div[@class='epl-tab-section']/div/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='epl-slider-slides']/div/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        parking =response.xpath("normalize-space(//span[@title='Parking Spaces']/span/text())").extract_first()    
        if parking:
            (item_loader.add_value("parking", True) if parking !="0" else item_loader.add_value("parking", False))

        dishwasher ="".join(response.xpath("//ul[@class='listing-info epl-tab-2-columns']/li[@class='dishwasher']/text()").extract())   
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        balcony ="".join(response.xpath("//ul[@class='listing-info epl-tab-2-columns']/li[@class='balcony']/text()").extract())   
        if balcony:
            item_loader.add_value("balcony", True)

        if response.xpath("//div[@class='author-contact-details']//span[@class='mobile']/text()"): item_loader.add_xpath("landlord_phone", "normalize-space(//div[@class='author-contact-details']//span[@class='mobile']/text())")
        else: item_loader.add_value("landlord_name", "Lisa McConaghy")
        
        if response.xpath("//div[@class='author-contact-details']/h5/a/text()").get(): item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='author-contact-details']/h5/a/text())")
        else: item_loader.add_value("landlord_phone","0417 229 202")
        
        if response.xpath("//div[@class='author-social-buttons']/a/text()").get(): item_loader.add_xpath("landlord_email", "normalize-space(//div[@class='author-social-buttons']/a/text())")  
        else: item_loader.add_value("landlord_email","rentals1@rodneymorley.com.au")

        yield item_loader.load_item()