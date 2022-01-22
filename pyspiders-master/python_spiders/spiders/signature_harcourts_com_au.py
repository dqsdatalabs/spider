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

class MySpider(Spider):
    name = 'signature_harcourts_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=3&min=&max=&minbed=&maxbed=",
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=6&min=&max=&minbed=&maxbed=",
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=13&min=&max=&minbed=&maxbed=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=5&min=&max=&minbed=&maxbed=",
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=7&min=&max=&minbed=&maxbed=",
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=11&min=&max=&minbed=&maxbed=",
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=12&min=&max=&minbed=&maxbed=",
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=14&min=&max=&minbed=&maxbed=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://signature.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=10&min=&max=&minbed=&maxbed=",
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

        for item in response.xpath("//div[@class='listingContent']/h2/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='pagerNext']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "Rentals?pageid" in response.url:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Signature_Harcourts_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@id,'pageTitle')]/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@id,'pageTitle')]/h1/text()", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@id,'detailTitle')]/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'read-more-wrap')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[@class='bdrm']/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[@class='bthrm']/span/text()", input_type="F_XPATH", get_num=True)
        rent = response.xpath("//h3[contains(@id,'Price')]/text()").get()
        if rent and "pw" in rent.replace(".",""):
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h3[contains(@id,'Price')]/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        elif rent:
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h3[contains(@id,'Price')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/span[contains(.,'Available')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li/span[contains(.,'Bond')]/following-sibling::text()", input_type="F_XPATH", get_num=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span/strong[contains(.,'Listing')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'images-modal-carousel')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//iframe/@src[contains(.,'map')]", input_type="F_XPATH", split_list={"center=":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//iframe/@src[contains(.,'map')]", input_type="F_XPATH", split_list={"center=":1,",":1,"&":0})
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//li[contains(.,'Pets') and not(contains(.,'No Pets'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(@title,'Carport')]/span/text() | //li[contains(@title,'car')]/span/text() | //li[contains(@class,'grge')]/span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,' furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//li[@class='agentContent']/h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//li[@class='dLarge']/a[contains(@href,'tel')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="signature@harcourts.com.au", input_type="VALUE")
        
        yield item_loader.load_item()