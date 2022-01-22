# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n
class MySpider(Spider):
    name = 'jacksongreenpreston_co_uk' 
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'        
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.jacksongreenpreston.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&location=&propertyType=28&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&recentlyAdded=&availability=0%2C1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.jacksongreenpreston.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&location=&propertyType=1%2C2%2C3%2C4%2C26&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&recentlyAdded=&availability=0%2C1",
                    "https://www.jacksongreenpreston.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&propertyAge=&location=&propertyType=12&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&recentlyAdded=&availability=0%2C1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='wp-block-img']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[.='›']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        status = response.xpath("//div[contains(@class,'full_description_small')]//text()[contains(.,'Single parking space') or contains(.,'Single car parking space') ]").get()
        if status:
            return
        removegarage=response.xpath("//div[contains(@class,'full_description_small ul-default')]//text()[contains(.,'Single garage') or contains(.,'parking space')]").get()
        if removegarage:
            return

        item_loader.add_value("external_id", response.url.split("/")[-1])       

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Jacksongreenpreston_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='row']//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='row']//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='row']//h1/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='row']//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'full_description_large')]//text()[.!='Read less']", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='row']//h4/text()[contains(.,'Bedroom')]", input_type="F_XPATH", get_num=True,split_list={"Bedroom":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='thumbnail_images']/div/a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='floorplan']//div[@class='tab-content']//p//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[@id='street-view']//div/@data-location", input_type="F_XPATH",split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[@id='street-view']//div/@data-location", input_type="F_XPATH",split_list={",":1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='row']//p/text()[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//ul[@class='ul-default']/li[contains(.,'/EPC')]//text()", input_type="F_XPATH",split_list={"/EPC":0," ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[strong[.='Contact Info']]/li/h3//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[strong[.='Contact Info']]/li//a[contains(@href,'tel')]//text()", input_type="F_XPATH",split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[strong[.='Contact Info']]/li//a[contains(@href,'mail')]//text()", input_type="F_XPATH")
        rent = response.xpath("//p[@class='price']/text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split('£')[-1].split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent) 

        deposit = response.xpath("//h4[contains(.,'Key Features')]//following-sibling::ul//li[contains(.,'Deposit')]//text()").get()
        if deposit and "£" in deposit:
            deposit = deposit.split("Deposit")[1].replace("£","").split("/")[0].strip()
            item_loader.add_value("deposit", deposit)
        bathroom = response.xpath("//h4[contains(.,'Key Features')]//following-sibling::ul//li[contains(.,'bathroom') or contains(.,'Bathroom')]//text()").get()
        if bathroom:
            bathroom = bathroom.lower().replace("family ","").split("bedrooms/bathroom")[0].split("bathroom")[0].replace("spacious","").strip().split(" ")[-1]
            if bathroom.isdigit():
                item_loader.add_value("bathroom_count", bathroom)
            elif bathroom.isalpha():
                try:
                    bathroom_count = w2n.word_to_num(bathroom)
                    item_loader.add_value("bathroom_count", bathroom_count)
                except:
                    pass
            
        furnished = response.xpath("//ul[@class='ul-default']/li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        if furnished:
            if "unfurnished" not in furnished.lower():
                item_loader.add_value("furnished", True)

        yield item_loader.load_item()