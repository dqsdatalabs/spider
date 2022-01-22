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
    name = 'belleproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.belleproperty.com/listings/?category=Unit&category=Apartment&category=Flat&price_max=&listing_type=Lease&baths_min=&building_area_min=&beds_min=&property_status=Available&rent_min=&land_area_min=&price_min=&q=&surrounding_suburbs=True&property_type=ResidentialLease&rent_max=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.belleproperty.com/listings/?site_id=2&latitude=&longitude=&is_building=&is_project_listing=&property_type=ResidentialLease&listing_type=Lease&property_status=Available&sale_type=&view=list&category=House&category=Townhouse&category=Villa&category=DuplexSemi-detached&category=Terrace&state=&region=&q=&beds_min=&baths_min=&price_min=&rent_min=&price_max=&rent_max=&sort_by=&surrounding_suburbs=on",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.belleproperty.com/listings/?site_id=2&latitude=&longitude=&is_building=&is_project_listing=&property_type=ResidentialLease&listing_type=Lease&property_status=Available&sale_type=&view=list&category=Studio&state=&region=&q=&beds_min=&baths_min=&price_min=&rent_min=&price_max=&rent_max=&sort_by=&surrounding_suburbs=on",
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

        for item in response.xpath("//div[@class='listing-caption']/following-sibling::a[1]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status =response.xpath("(//div[contains(@class,'price')]//text())[1]").get() 
        if status and "deposit taken" in status.lower() or "short term" in status.lower() or "deposit received" in status.lower() :
            return
        new_status =response.xpath("//div[@class='listing-price show-for-large']//text()").get() 
        if new_status and "under" in new_status.lower():
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Belleproperty_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[@class='sub-heading']/text() | //span[@class='main-heading']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='main-heading']/text()", input_type="F_XPATH")
        zipcode=response.xpath("//title//text()").get()
        if zipcode:
            zipcode=zipcode.split(" ")[-2:]
            item_loader.add_value("zipcode",zipcode)
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h3[contains(@class,'main')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='rich-text']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='rich-text']//p//text()[contains(.,'sqm')]", input_type="F_XPATH", get_num=True, split_list={"sqm":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='icon-bed']/preceding-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='icon-bath']/preceding-sibling::text()", input_type="F_XPATH", get_num=True)
        dontallow=response.xpath("//div[@class='listing-price']/text()[contains(.,'Application Approved')]").get()
        if dontallow:
            return 
        rent = response.xpath("//div[contains(@class,'price')]/text()").get()
        if rent:
            if "DEPOSIT" in rent:
                return
            if "Buyers Guide" in rent or "LEASED" in rent:
                return 
            if "$" in rent:
                rent = rent.split("$")[1].strip().split(" ")[0].lower().replace(",","").replace("pw","").replace("per","")
                if "." in rent:
                    rent = rent.split(".")[0]
                elif "-" in rent:
                    rent = rent.split("-")[0]
                elif "|" in rent: 
                    rent = rent.split("|")[0]
                elif "/" in rent:
                    rent = rent.split("/")[0]
                    
                    
                item_loader.add_value("rent", int(rent)*4)

        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[contains(@class,'smallport')][contains(.,'Available')]/following-sibling::div/strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[contains(@class,'smallport')][contains(.,'Bond')]/following-sibling::div/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@class,'smallport')][contains(.,'ID')]/following-sibling::div/strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//figure[contains(@class,'swiper-slide')]//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//a/@href[contains(.,'Location')]", input_type="F_XPATH", split_list={"daddr=":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//a/@href[contains(.,'Location')]", input_type="F_XPATH", split_list={"daddr=":1,",":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[@class='icon-car']/preceding-sibling::text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[@class='rich-text']//p//text()[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//h3[contains(.,'FURNISHED') or contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='agent-name']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='agent-mobile']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@belleproperty.com", input_type="VALUE")
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            item_loader.add_value("landlord_phone","0281 169 444")

        yield item_loader.load_item()
