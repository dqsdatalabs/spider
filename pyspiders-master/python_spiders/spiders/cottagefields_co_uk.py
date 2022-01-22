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
    name = 'cottagefields_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cottagefields.co.uk/notices?c=44|46|48&p={}&q=&filter_attribute[numeric][2][min]=&price_sort=&filter_attribute[categorical][1]=Flat&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&min_price=&max_price=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cottagefields.co.uk/notices?c=44|46|48&p={}&q=&filter_attribute[numeric][2][min]=&price_sort=&filter_attribute[categorical][1]=Detached&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&min_price=&max_price=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=",
                    "https://www.cottagefields.co.uk/notices?c=44|46|48&p={}&q=&filter_attribute[numeric][2][min]=&price_sort=&filter_attribute[categorical][1]=Detached+Bungalow&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&min_price=&max_price=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=",
                    "https://www.cottagefields.co.uk/notices?c=44|46|48&p={}&q=&filter_attribute[numeric][2][min]=&price_sort=&filter_attribute[categorical][1]=Terraced&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&min_price=&max_price=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=",
                    "https://www.cottagefields.co.uk/notices?c=44|46|48&p={}&q=&filter_attribute[numeric][2][min]=&price_sort=&filter_attribute[categorical][1]=Semi+Detached&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&min_price=&max_price=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.cottagefields.co.uk/notices?c=44|46|48&p={}&q=&filter_attribute[numeric][2][min]=&price_sort=&filter_attribute[categorical][1]=Studio&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&min_price=&max_price=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='card']"):
            status = item.xpath("./div[@class='let_agreed' or @class='let']/text()").get()
            if status and "to rent" not in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cottagefields_Co_PySpider_united_kingdom")      
        title =response.xpath("//h3[@class='page-title']//text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip() )   
        external_id =response.xpath("//div[b[contains(.,'Reference number')]]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip() )  
        address =" ".join(response.xpath("//div[@class='map_location']//text()").extract())
        if address:
            address = address.split(" - ")[0].strip() 
            item_loader.add_value("address",address.strip() ) 
            if len(address.split(",")) > 1:
                zipcode =  address.split(",")[-1].strip()
                if zipcode.isalpha():
                   item_loader.add_value("city", address.split(",")[-1].strip()) 
                else:
                    item_loader.add_value("city", address.split(",")[-2].strip())
                    item_loader.add_value("zipcode",zipcode)
    
        item_loader.add_xpath("rent_string","//span[@class='page_title_price']//text()")                
        
        available_date = response.xpath("//div[b[contains(.,'Available from ')]]/text()").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        room_count = response.xpath("//div[contains(@class,'key-features')]/p/text()[contains(.,'Bedroom')]").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.split("Bedroom")[0].strip().split(" ")[-1])  
        
        bathroom_count = response.xpath("//div[contains(@class,'key-features')]/p/text()[contains(.,'Bathroom')]").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.split("Bathroom")[0].strip().split(" ")[-1])  
    
        washing_machine = response.xpath("//div[contains(@class,'key-features')]/div/p/text()[contains(.,'Washing Machine')]").extract_first()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        balcony = response.xpath("//div[contains(@class,'key-features')]/div/p/text()[contains(.,'Balcony')]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//div[contains(@class,'key-features')]/div/p/text()[contains(.,'Parking')]").extract_first()
        if parking:
            item_loader.add_value("parking",True)    
            
        desc = " ".join(response.xpath("//div[@id='full_notice_description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@id='royalSlider']/div/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//input[contains(@id,'floor_plan')]/@value").extract()]
        if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images) 
        
        item_loader.add_value("landlord_name", "Cottage Fields")
        item_loader.add_value("landlord_phone", "02036 218 621")
        
        yield item_loader.load_item()