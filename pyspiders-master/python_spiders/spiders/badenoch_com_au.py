# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'badenoch_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://badenoch.com.au/rent/property-for-rent?streetsuburbRentals=&minpricer=0&maxpricer=0&listingtype%5B%5D=Apartment&qt=search&useSearchType=search&orderBy=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://badenoch.com.au/rent/property-for-rent?streetsuburbRentals=&minpricer=0&maxpricer=0&listingtype%5B%5D=House&listingtype%5B%5D=Townhouse&qt=search&useSearchType=search&orderBy=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='listingWrap']/div/div[1]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//small[contains(@class,'glyphicon-chevron-right')]/../@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split(",")[-1])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Badenoch_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[contains(@class,'location')]//text()", input_type="F_XPATH")        
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[contains(@class,'location')]//text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[contains(@class,'location')]//text()", input_type="F_XPATH", split_list={",":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h3[contains(@class,'header')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='fix']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//a[contains(@title,'Bed')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//a[contains(@title,'Bath')]//text()", input_type="F_XPATH", get_num=True)
        
        price = response.xpath("//h3[contains(@class,'header')]//span//text()").get()
        if price:
            rent = price.split("$")[1].split(" ")[0].replace(",","")
            item_loader.add_value("rent", int(float(rent))*4)

        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p[contains(.,'Available')]//strong//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'gall-main-slider')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//i[contains(@class,'ro-floorplan')]//parent::a//@href", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//a[contains(@title,'Car')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Pool')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher')]//text()", input_type="F_XPATH", tf_item=True)
        
        energy_label = response.xpath("//li[contains(.,'Energy Rating')]//text()").get()
        if energy_label:
            energy_label = energy_label.split(":")[1]
            item_loader.add_value("energy_label", energy_label)
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//h5[contains(@class,'agent-name')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//i[contains(@class,'phone')]//following-sibling::span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="	clientservices@badenoch.com.au", input_type="VALUE")
        
        yield item_loader.load_item()